package no.nav.tsm.ktor.kafka.config

import io.ktor.server.application.*
import io.ktor.server.plugins.di.*

class KafkaConfigPluginConfig {
    /** Used internally in kafka for tracing and logging, set it to the pod name. */
    lateinit var clientId: String
}

/**
 * If you are using consumers or producers without using either KafkaConsumerPlugin or KafkaProducerPlugin, you may use
 * this plugin to configure Ktor with the required kafka-configuration.
 */
val KafkaConfig =
    createApplicationPlugin(name = "KafkaBasePlugin", ::KafkaConfigPluginConfig) {
        val config = pluginConfig
        application.dependencies {
            provide<InternalKafkaConfig> { this@createApplicationPlugin.application.kafkaConfig(config.clientId) }
        }
    }
