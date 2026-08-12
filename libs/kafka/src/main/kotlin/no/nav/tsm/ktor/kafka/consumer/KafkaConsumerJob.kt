package no.nav.tsm.ktor.kafka.consumer

import io.ktor.server.application.Application
import io.ktor.server.plugins.di.dependencies
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import no.nav.tsm.ktor.kafka.config.InternalKafkaConfig
import no.nav.tsm.ktor.kafka.config.kafkaObjectMapper
import no.nav.tsm.ktor.logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import tools.jackson.databind.JacksonModule

internal class KafkaConsumerJobConfig(
    val groupId: String,
    val pollDuration: Duration,
    val retryDuration: Duration,
    val closeTimeout: Duration,
    val shutdownTimeout: Duration,
    val jacksonModules: MutableList<JacksonModule> = mutableListOf(),
)

/** Startable and stoppable consumer with manual committing and retry-mechanisms. */
class KafkaConsumerJob
private constructor(
    private val handlers: List<KafkaTopic<*>>,
    private val jobConfig: KafkaConsumerJobConfig,
    kafkaConfig: InternalKafkaConfig,
) {
    companion object {
        private val logger = logger()

        /** Automatically inject kafka config and initialize a KafkaConsumerJob */
        internal fun initConsumerJob(
            application: Application,
            handlers: List<KafkaTopic<*>>,
            jobConfig: KafkaConsumerJobConfig,
        ): KafkaConsumerJob {
            val kafkaConfig: InternalKafkaConfig by application.dependencies

            return KafkaConsumerJob(
                handlers = handlers,
                kafkaConfig = kafkaConfig,
                jobConfig = jobConfig,
            )
        }
    }

    private val topics = handlers.map { it.topic }
    private val objectMapper = kafkaObjectMapper.rebuild().addModules(jobConfig.jacksonModules).build()
    private val stopped = CompletableDeferred<Unit>()
    private val stopping: Boolean
        get() = stopped.isCompleted

    private val consumer: ByteArrayConsumer =
        ByteArrayConsumer(
            kafkaConfig.clientId,
            jobConfig.groupId,
            kafkaConfig,
        )

    suspend fun start() =
        withContext(Dispatchers.IO) {
            try {
                while (isActive) {
                    logger.debug("Subscribing to topics: ${topics.joinToString(", ")}")
                    consumer.subscribe(topics)
                    try {
                        while (isActive) {
                            val polled = consumer.poll(jobConfig.pollDuration.toJavaDuration())
                            if (polled.isEmpty) {
                                logger.debug("Got no records after ${jobConfig.pollDuration}, continuing to poll")
                                continue
                            }

                            polled.nextOffsets().forEach { (tp, nextOffsetMetadata) ->
                                val records = polled.records(tp)
                                val handler = handlers.first { handler -> handler.topic == tp.topic() }

                                when (handler) {
                                    is KafkaTopic.Unbatched -> handleRecordsSingle(handler, records)
                                    is KafkaTopic.Batched<*> ->
                                        handleRecordsMultiple(handler, records, mapOf(tp to nextOffsetMetadata))
                                }
                            }
                        }
                    } catch (ex: WakeupException) {
                        logger.debug("Kafka consumer woken up, stopping")
                        throw ex
                    } catch (ex: CancellationException) {
                        logger.debug("Kafka consumer cancelled gracefully (application stopping)", ex)
                        throw ex
                    } catch (ex: KafkaParseException) {
                        unsubscribeAndRetry("Parsing of record (${ex.meta.description()}) failed", ex)
                    } catch (ex: KafkaHandlerException) {
                        unsubscribeAndRetry(
                            "Handling of record(s) (count: ${ex.meta.size}) (first: ${ex.meta.first().description()}) failed",
                            ex,
                        )
                    } catch (ex: Exception) {
                        unsubscribeAndRetry("Unknown error running Kafka consumer", ex)
                    }
                }
            } catch (ex: WakeupException) {
                logger.info("Kafka consumer job stopped (Wakeup)")
            } finally {
                logger.info("Stopping consumer job")
                close()
            }
        }

    private suspend fun handleRecordsSingle(
        handler: KafkaTopic.Unbatched<*>,
        records: MutableIterable<ConsumerRecord<String, ByteArray?>>,
    ) {
        for (record in records) {
            val meta = record.toRecordMeta()
            val value = record.value()
            if (value == null) {
                logger.debug("Received tombstone for key ${record.key()} on topic ${meta.topic}")
                handler.onTombstone(meta)
                commitSync(record.nextOffsets())
                continue
            }

            try {
                handler.handleRecord(value, meta, objectMapper)
                /* If handleRecord fails, sync is skipped and error propagates to  KafkaHandlerException */
                commitSync(record.nextOffsets())
            } catch (ex: Exception) {
                if (handler.shouldSkip?.invoke(meta) == true) {
                    logger.info(
                        "Record ${meta.key} on topic ${meta.topic} failed with exception, but shouldSkip returned true, skipping",
                        ex,
                    )
                    commitSync(record.nextOffsets())
                }
                throw ex
            }
        }
    }

    private suspend fun handleRecordsMultiple(
        handler: KafkaTopic.Batched<*>,
        records: MutableIterable<ConsumerRecord<String, ByteArray?>>,
        nextRecords: Map<TopicPartition, OffsetAndMetadata>,
    ) {
        val recordsWithMeta = records.map { it.value() to it.toRecordMeta() }
        logger.debug(
            "Batched topic (${handler.topic}) received ${recordsWithMeta.size} records, of which ${recordsWithMeta.count { it.first == null }} are tombstones"
        )
        handler.handleRecords(recordsWithMeta, objectMapper)
        /**
         * All records in a batch needs to be processed OK, this is up to handleRecords throwing or not.
         *
         * If handleRecords throws for any reason, none of the records will be committed and the entire batch will be
         * re-processed after the given retry-timeout..
         */
        commitSync(nextRecords)
    }

    private fun commitSync(nextOffsets: Map<TopicPartition, OffsetAndMetadata>) {
        try {
            consumer.commitSync(nextOffsets)
        } catch (ex: WakeupException) {
            logger.info("Shutdown interrupted the commit of $nextOffsets, committing before stopping")
            try {
                consumer.commitSync(nextOffsets)
            } catch (e: Exception) {
                logger.error(
                    "Failed to commit offsets $nextOffsets while shutting down",
                    e,
                )
            }
            throw ex
        }
    }

    fun stop() {
        logger.info("Stopping Kafka consumer")
        stopped.complete(Unit)
        consumer.wakeup()
    }

    fun close() {
        try {
            logger.debug("Closing Kafka consumer")
            consumer.close(jobConfig.closeTimeout.toJavaDuration())
        } catch (ex: Exception) {
            logger.warn("Error while closing Kafka consumer", ex)
        }
    }

    private suspend fun unsubscribeAndRetry(message: String, cause: Throwable) {
        if (stopping) {
            logger.error("$message. The consumer is stopping, so it is handled again after restart", cause)
            return
        } else {
            logger.error("$message, retrying after ${jobConfig.retryDuration}", cause)
        }

        try {
            consumer.unsubscribe()
        } catch (ex: WakeupException) {
            throw ex
        } catch (ex: Exception) {
            logger.warn("Failed to unsubscribe before retrying, continuing anyway", ex)
        }

        withTimeoutOrNull(jobConfig.retryDuration) { stopped.await() }
    }
}

private fun ConsumerRecord<String, ByteArray?>.nextOffsets(): Map<TopicPartition, OffsetAndMetadata> {
    return mapOf(TopicPartition(this.topic(), this.partition()) to OffsetAndMetadata(this.offset() + 1))
}
