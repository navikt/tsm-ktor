package no.nav.tsm.ktor.kafka.producer

import io.ktor.server.application.*
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import tools.jackson.databind.JacksonModule

class KafkaProducerPluginConfig {
    lateinit var clientId: String
}

val KafkaProducer =
    createApplicationPlugin(name = "KafkaProducerPlugin", ::KafkaProducerPluginConfig) {
        try {
            /** The base plugin can have been installed by one of the other plugins, which is fine. */
            application.install(KafkaConfig) {
                clientId = pluginConfig.clientId
            }
        } catch (_: DuplicatePluginException) {
            // Already installed, no worries
        }
    }

fun <Payload> Application.createProducer(
    topic: String,
    jacksonModules: List<JacksonModule> = emptyList(),
) =
    KafkaRecordProducer.initProducer<Payload>(
        application = this,
        topic = topic,
        jacksonModules = jacksonModules,
    )
