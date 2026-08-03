package no.nav.tsm.ktor.kafka.config

import io.ktor.server.application.*
import io.ktor.server.plugins.di.*

internal class KafkaBaseConfig {
    /** Used internally in kafka for tracing and logging, set it to the pod name. */
    lateinit var clientId: String
}

internal val KafkaBase =
    createApplicationPlugin(name = "KafkaBasePlugin", ::KafkaBaseConfig) {
        val config = pluginConfig
        application.dependencies {
            provide<KafkaConfig> { this@createApplicationPlugin.application.kafkaConfig(config.clientId) }
        }
    }
