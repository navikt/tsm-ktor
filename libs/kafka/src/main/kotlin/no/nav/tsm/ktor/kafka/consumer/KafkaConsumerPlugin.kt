package no.nav.tsm.ktor.kafka.consumer

import io.ktor.server.application.*
import io.ktor.server.application.hooks.*
import java.util.*
import kotlinx.coroutines.*
import no.nav.tsm.ktor.kafka.config.KafkaConfig

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
                application.install(KafkaConfig) {
                    clientId = pluginConfig.clientId
                }
            } catch (_: DuplicatePluginException) {
                // Already installed, no worries
            }

            val job =
                KafkaConsumerJob.initConsumerJob(
                    application = application,
                    handlers = pluginConfig.topics,
                    jobConfig =
                        KafkaConsumerJobConfig(
                            groupId = pluginConfig.groupId,
                            pollDuration = pluginConfig.pollDuration,
                            retryDuration = pluginConfig.retryDuration,
                            jacksonModules = pluginConfig.jacksonModules.toMutableList(),
                        ),
                )

            on(MonitoringEvent(ApplicationStarted)) { application ->
                application.launch {
                    job.start()
                }
            }

            on(MonitoringEvent(ApplicationStopped)) {
                job.stop()
            }
        }
