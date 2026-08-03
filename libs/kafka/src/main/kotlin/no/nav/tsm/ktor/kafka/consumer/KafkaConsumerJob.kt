package no.nav.tsm.ktor.kafka.consumer

import kotlin.time.toJavaDuration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import no.nav.tsm.ktor.kafka.config.kafkaObjectMapper
import no.nav.tsm.ktor.logger

/** Startable and stoppable consumer with manual committing and retry-mechanisms. */
internal class KafkaConsumerJob(
    val topics: List<String>,
    val consumer: ByteArrayConsumer,
    val pluginConfig: KafkaConsumerPluginConfig,
) {
    val logger = logger()
    val objectMapper =
        kafkaObjectMapper().apply {
            pluginConfig.jacksonModules.forEach { registerModule(it) }
        }

    suspend fun start() =
        withContext(Dispatchers.IO) {
            while (isActive) {
                logger.debug("Subscribing to topics: ${topics.joinToString(", ")}")
                consumer.subscribe(topics)
                try {
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
                                consumer.commitSync(topic, record)
                                continue
                            }

                            handler.handleRecord(value, meta, objectMapper)
                            /* If handleRecord fails, sync is skipped and error propagates to  KafkaHandlerException */
                            consumer.commitSync(topic, record)
                        }
                    }
                } catch (ex: CancellationException) {
                    logger.debug("Kafka consumer cancelled gracefully (application stopping)", ex)
                } catch (ex: KafkaParseException) {
                    unsubscribeAndRetry(
                        "Parsing of record (${ex.meta.description()}) failed, retrying after ${pluginConfig.retryDuration}",
                        ex,
                    )
                } catch (ex: KafkaHandlerException) {
                    unsubscribeAndRetry(
                        "Handling of record (${ex.meta.description()}) failed, retrying after ${pluginConfig.retryDuration}",
                        ex,
                    )
                } catch (ex: Exception) {
                    unsubscribeAndRetry(
                        "Unknown error running Kafka consumer, waiting ${pluginConfig.retryDuration} to retry",
                        ex,
                    )
                }
            }
        }

    fun stop() {
        logger.debug("Stopping Kafka consumer")
        consumer.unsubscribe()
    }

    private suspend fun unsubscribeAndRetry(message: String, cause: Throwable) {
        logger.error(message, cause)
        consumer.unsubscribe()
        delay(pluginConfig.retryDuration)
    }
}
