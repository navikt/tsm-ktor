package no.nav.tsm.ktor.kafka.consumer

import io.ktor.server.application.*
import io.ktor.server.application.hooks.*
import io.ktor.server.plugins.di.dependencies
import java.util.*
import kotlinx.coroutines.*
import no.nav.tsm.ktor.kafka.config.KafkaBase
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import no.nav.tsm.ktor.kafka.config.kafkaConfig

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

            val kafkaConfig: KafkaConfig by application.dependencies
            val configuredTopics: List<String> = pluginConfig.topics.map { it.topic }
            val byteArrayConsumer =
                ByteArrayConsumer(
                    pluginConfig.clientId,
                    pluginConfig.groupId,
                    kafkaConfig,
                )
            val job = KafkaConsumerJob(configuredTopics, byteArrayConsumer, pluginConfig)

            on(MonitoringEvent(ApplicationStarted)) { application ->
                application.launch {
                    job.start()
                }
            }

            on(MonitoringEvent(ApplicationStopped)) {
                job.stop()
            }
        }
