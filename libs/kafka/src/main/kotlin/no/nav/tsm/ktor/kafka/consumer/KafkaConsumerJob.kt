package no.nav.tsm.ktor.kafka.consumer

import kotlin.time.toJavaDuration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import no.nav.tsm.ktor.kafka.config.kafkaObjectMapper
import no.nav.tsm.ktor.logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.WakeupException

/** Startable and stoppable consumer with manual committing and retry-mechanisms. */
internal class KafkaConsumerJob(
    private val topics: List<String>,
    private val consumer: ByteArrayConsumer,
    private val pluginConfig: KafkaConsumerPluginConfig,
) {
    private val logger = logger()
    private val objectMapper =
        kafkaObjectMapper().apply {
            pluginConfig.jacksonModules.forEach { registerModule(it) }
        }

    private val stopped = CompletableDeferred<Boolean>()

    private val stopping: Boolean
        get() = stopped.isCompleted

    suspend fun start() =
        withContext(Dispatchers.IO) {
            try {
                while (isActive && !stopping) {
                    try {
                        logger.debug("Subscribing to topics: ${topics.joinToString(", ")}")
                        consumer.subscribe(topics)
                        while (isActive) {
                            val records = consumer.poll(pluginConfig.pollDuration.toJavaDuration())
                            if (records.isEmpty) {
                                logger.debug("Got no records after ${pluginConfig.pollDuration}, continuing to poll")
                                continue
                            }

                            for (record in records) {
                                val topic = record.topic()
                                val handler = pluginConfig.topics.find { it.topic == topic }
                                requireNotNull(handler) {
                                    "Topic $topic was subscribed, but found no configuration with onRecord for it."
                                }

                                val meta = record.toRecordMeta()
                                val value = record.value()
                                if (value == null) {
                                    logger.debug("Received tombstone for key ${record.key()} on topic $topic")
                                    handler.onTombstone(meta)
                                    commitSync(topic, record)
                                    continue
                                }

                                handler.handleRecord(value, meta, objectMapper)
                                /* If handleRecord fails, sync is skipped and error propagates to  KafkaHandlerException */
                                commitSync(topic, record)
                            }
                        }
                    } catch (ex: WakeupException) {
                        throw ex
                    } catch (ex: CancellationException) {
                        logger.debug("Kafka consumer coroutine cancelled, shutting down", ex)
                        throw ex
                    } catch (ex: KafkaParseException) {
                        unsubscribeAndRetry("Parsing of record (${ex.meta.description()}) failed", ex)
                    } catch (ex: KafkaHandlerException) {
                        unsubscribeAndRetry("Handling of record (${ex.meta.description()}) failed", ex)
                    } catch (ex: Exception) {
                        unsubscribeAndRetry("Unknown error running Kafka consumer", ex)
                    }
                }
            } catch (ex: WakeupException) {
                logger.info("Kafka consumer woken up, shutting down")
            } finally {
                logger.info("Kafka consumer shutting down")
                close()
            }
        }

    private fun commitSync(topic: String, record: ConsumerRecord<String, ByteArray?>) {
        try {
            consumer.commitSync(topic, record)
        } catch (ex: WakeupException) {
            logger.info("Shutdown interrupted the commit of $topic-${record.partition()}, committing before stopping")
            try {
                consumer.commitSync(topic, record, pluginConfig.closeTimeout.toJavaDuration())
            } catch (e: Exception) {
                logger.error(
                    "Failed to commit offset ${record.offset() + 1} for $topic-${record.partition()} while shutting down",
                    e,
                )
            }
            throw ex
        }
    }

    private suspend fun unsubscribeAndRetry(failure: String, cause: Throwable) {
        if (stopping) {
            logger.error("$failure. The consumer is stopping, so it is handled again after restart", cause)
            return
        } else {
            logger.error("$failure, retrying after ${pluginConfig.retryDuration}", cause)
        }

        try {
            consumer.unsubscribe()
        } catch (ex: WakeupException) {
            throw ex
        } catch (ex: Exception) {
            logger.warn("Failed to unsubscribe before retrying, continuing anyway", ex)
        }

        withTimeoutOrNull(pluginConfig.retryDuration) { stopped.await() }
    }

    fun stop() {
        logger.info("Stopping Kafka consumer")
        stopped.complete(true)
        consumer.wakeup()
    }

    fun close() {
        try {
            logger.debug("Closing Kafka consumer")
            consumer.close(pluginConfig.closeTimeout.toJavaDuration())
        } catch (ex: Exception) {
            logger.warn("Error while closing Kafka consumer", ex)
        }
    }
}
