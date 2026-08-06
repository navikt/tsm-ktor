package no.nav.tsm.ktor.kafka.consumer

import io.ktor.server.application.Application
import io.ktor.server.plugins.di.dependencies
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import no.nav.tsm.ktor.kafka.config.InternalKafkaConfig
import no.nav.tsm.ktor.kafka.config.kafkaObjectMapper
import no.nav.tsm.ktor.logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import tools.jackson.databind.JacksonModule

internal class KafkaConsumerJobConfig(
    val groupId: String,
    val pollDuration: Duration,
    val retryDuration: Duration,
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

    private val consumer: ByteArrayConsumer =
        ByteArrayConsumer(
            kafkaConfig.clientId,
            jobConfig.groupId,
            kafkaConfig,
        )

    suspend fun start() =
        withContext(Dispatchers.IO) {
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

                        handlers.forEach { handler ->
                            val records = polled.records(handler.topic)

                            when (handler) {
                                is KafkaTopic.Unbatched -> handleRecordsSingle(handler, records)
                                is KafkaTopic.Batched<*> -> handleRecordsMultiple(handler, records)
                            }
                        }
                    }
                } catch (ex: CancellationException) {
                    logger.debug("Kafka consumer cancelled gracefully (application stopping)", ex)
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
                consumer.commitSync(meta.topic, record)
                continue
            }

            try {
                handler.handleRecord(value, meta, objectMapper)
                /* If handleRecord fails, sync is skipped and error propagates to  KafkaHandlerException */
                consumer.commitSync(meta.topic, record)
            } catch (ex: Exception) {
                if (handler.shouldSkip?.invoke(meta) == true) {
                    logger.info(
                        "Record ${meta.key} on topic ${meta.topic} failed with exception, but shouldSkip returned true, skipping",
                        ex,
                    )
                    consumer.commitSync(meta.topic, record)
                }

                // No shouldSkip configured, or shouldSkip returned false, proceed with normal error handling
                throw ex
            }
        }
    }

    private suspend fun handleRecordsMultiple(
        handler: KafkaTopic.Batched<*>,
        records: MutableIterable<ConsumerRecord<String, ByteArray?>>,
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
        consumer.commitSync(handler.topic, records.last())
    }

    fun stop() {
        logger.debug("Stopping Kafka consumer")
        consumer.unsubscribe()
    }

    private suspend fun unsubscribeAndRetry(message: String, cause: Throwable) {
        logger.error("${message}, retrying after ${jobConfig.retryDuration}", cause)
        consumer.unsubscribe()
        delay(jobConfig.retryDuration)
    }
}
