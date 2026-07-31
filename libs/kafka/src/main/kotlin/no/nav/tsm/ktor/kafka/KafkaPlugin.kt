package no.nav.tsm.ktor.kafka

import io.ktor.server.application.*
import io.ktor.server.application.hooks.*
import java.util.UUID
import kotlin.time.toJavaDuration
import kotlinx.coroutines.*
import no.nav.tsm.ktor.logger

/**
 * Installs a consumer and attaches to the Ktor life-cycle. The consumer can subscribe to many topics with each their
 * own handler and their own record type.
 *
 * The consumer handles committing to the appropriate topic, partition and offset, but only if the record was parsed
 * successfully, and the handler
 */
val KafkaConsumer: ApplicationPlugin<KafkaConsumerPluginConfig>
    get() =
        createApplicationPlugin(name = "KafkaConsumer-${UUID.randomUUID()}", ::KafkaConsumerPluginConfig) {
            val logger = logger()
            val configuredTopics: List<String> = pluginConfig.topics.map { it.topic }
            val consumer = ByteArrayConsumer(pluginConfig.groupId, application.kafkaConfig())

            val unsubscribeAndRetry: suspend (String, Throwable) -> Unit = { message, cause ->
                logger.error(message, cause)
                consumer.unsubscribe()
                delay(pluginConfig.retryDuration)
            }

            on(MonitoringEvent(ApplicationStarted)) { application ->
                application.launch {
                    withContext(Dispatchers.IO) {
                        while (isActive) {
                            logger.debug("Subscribing to topics: ${configuredTopics.joinToString(", ")}")
                            consumer.subscribe(configuredTopics)
                            try {
                                while (isActive) {
                                    val records = consumer.poll(pluginConfig.pollDuration.toJavaDuration())
                                    if (records.isEmpty) {
                                        logger.debug(
                                            "Got no records after ${pluginConfig.pollDuration}, continuing to poll"
                                        )
                                        continue
                                    }

                                    for (record in records) {
                                        val topic = record.topic()
                                        val handler = pluginConfig.topics.find { it.topic == topic }
                                        requireNotNull(handler) {
                                            "Topic $topic was subscribed, but found no configuration with onRecord for it."
                                        }

                                        val value = record.value()
                                        if (value == null) {
                                            logger.debug("Received tombstone for key ${record.key()} on topic $topic")
                                            handler.onTombstone(record.key())
                                            consumer.commitSync(topic, record)
                                            continue
                                        }

                                        try {
                                            handler.handleRecord(value)
                                            consumer.commitSync(topic, record)
                                        } catch (ex: Exception) {
                                            logger.error(
                                                "Error parsing record with key ${record.key()} on topic $topic",
                                                ex,
                                            )

                                            throw ex
                                        }
                                    }
                                }
                            } catch (ex: CancellationException) {
                                logger.info("Kafka consumer cancelled gracefully (application stopping)", ex)
                            } catch (ex: KafkaParseException) {
                                unsubscribeAndRetry(
                                    "Parsing of record on topic ${ex.topic} failed, retrying after ${pluginConfig.retryDuration}",
                                    ex,
                                )
                            } catch (ex: KafkaHandlerException) {
                                unsubscribeAndRetry(
                                    "Handling of record on topic ${ex.topic} failed, retrying after ${pluginConfig.retryDuration}",
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
                }
            }

            on(MonitoringEvent(ApplicationStopped)) {
                consumer.unsubscribe()
            }
        }
