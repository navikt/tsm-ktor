package no.nav.tsm.ktor.kafka.consumer

import io.ktor.server.application.*
import io.ktor.server.application.hooks.*
import io.ktor.server.plugins.di.*
import java.util.*
import kotlinx.coroutines.*
import no.nav.tsm.ktor.kafka.config.KafkaBase
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import no.nav.tsm.ktor.logger

private val logger = logger()

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
            try {
                /** The base plugin can have been installed by one of the other plugins, which is fine. */
                application.install(KafkaBase) {
                    clientId = pluginConfig.clientId
                }
            } catch (_: DuplicatePluginException) {
                // Already installed, no worries
            }
            val consumerScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
            val kafkaConfig: KafkaConfig by application.dependencies
            val configuredTopics: List<String> = pluginConfig.topics.map { it.topic }
            val byteArrayConsumer =
                ByteArrayConsumer(
                    pluginConfig.clientId,
                    pluginConfig.groupId,
                    kafkaConfig,
                )
            val kafkaConsumerJob = KafkaConsumerJob(configuredTopics, byteArrayConsumer, pluginConfig)
            var job: Job? = null
            on(MonitoringEvent(ApplicationStarted)) {
                job = consumerScope.launch {
                    try {
                        kafkaConsumerJob.start()
                    } catch (ex: CancellationException) {
                        throw ex
                    } catch (ex: Exception) {
                        logger.error(
                            "Kafka consumer for ${configuredTopics.joinToString(", ")} stopped and will not " +
                                "consume more records until the application is restarted",
                            ex,
                        )
                    }
                }
            }
            on(MonitoringEvent(ApplicationStopping)) {
                if (job == null) {
                    kafkaConsumerJob.close()
                    consumerScope.cancel()
                    return@on
                }

                kafkaConsumerJob.stop()
                try {
                    runBlocking {
                        val stopped = withTimeoutOrNull(pluginConfig.shutdownTimeout) { job.join() }
                        if (stopped == null) {
                            logger.warn(
                                "Kafka consumer did not stop within ${pluginConfig.shutdownTimeout}, cancelling it. " +
                                    "Records being handled may be delivered again after restart."
                            )
                            consumerScope.cancel()
                            withTimeoutOrNull(pluginConfig.closeTimeout) { job.join() }
                        }
                    }
                } finally {
                    consumerScope.cancel()
                }
            }
        }
