package no.nav.tsm.ktor.kafka.producer

import com.fasterxml.jackson.databind.Module
import io.ktor.server.application.*
import io.ktor.server.plugins.di.dependencies
import no.nav.tsm.ktor.kafka.config.KafkaBase
import no.nav.tsm.ktor.kafka.config.KafkaConfig

class KafkaProducerPluginConfig {
    lateinit var clientId: String
}

val KafkaProducer =
    createApplicationPlugin(name = "KafkaProducerPlugin", ::KafkaProducerPluginConfig) {
        try {
            /** The base plugin can have been installed by one of the other plugins, which is fine. */
            application.install(KafkaBase) {
                clientId = pluginConfig.clientId
            }
        } catch (_: DuplicatePluginException) {
            // Already installed, no worries
        }
    }

fun <Payload> Application.createProducer(
    topic: String,
    jacksonModules: List<Module> = emptyList(),
): KafkaRecordProducer<Payload> {
    val config: KafkaConfig by dependencies

    return KafkaRecordProducer(topic, config, jacksonModules)
}
