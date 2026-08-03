package no.nav.tsm.ktor.kafka.config

import com.typesafe.config.ConfigException
import io.ktor.server.application.Application
import io.ktor.server.config.ApplicationConfig
import java.util.Properties
import no.nav.tsm.ktor.logger
import no.nav.tsm.ktor.nais.RuntimeCluster
import no.nav.tsm.ktor.nais.getRuntimeCluster

private val logger = logger()

internal sealed interface InternalKafkaConfig {
    val clientId: String

    fun toProperties(): Properties

    class Local(override val clientId: String, val bootstrapServers: String = "localhost:9092") : InternalKafkaConfig {
        override fun toProperties() =
            Properties().apply {
                put("bootstrap.servers", bootstrapServers)
                put("security.protocol", "PLAINTEXT")
            }
    }

    class Cloud(
        override val clientId: String,
        val bootstrapServers: String,
        val truststoreLocation: String,
        val truststorePassword: String,
        val keystoreLocation: String,
        val keystorePassword: String,
    ) : InternalKafkaConfig {
        override fun toProperties() =
            Properties().apply {
                put("bootstrap.servers", bootstrapServers)
                put("security.protocol", "SSL")
                put("ssl.truststore.location", truststoreLocation)
                put("ssl.truststore.password", truststorePassword)
                put("ssl.truststore.type", "jks")
                put("ssl.keystore.location", keystoreLocation)
                put("ssl.keystore.password", keystorePassword)
                put("ssl.keystore.type", "PKCS12")
            }
    }

    class Raw(override val clientId: String, val config: Map<String, Any?>) : InternalKafkaConfig {
        override fun toProperties() = Properties().apply { putAll(config) }
    }
}

internal fun ApplicationConfig.kafkaConfig(clientId: String): InternalKafkaConfig {
    try {
        val confConfig = this.config("kafka.config").toMap()
        if (confConfig.isNotEmpty()) {
            return InternalKafkaConfig.Raw(clientId, confConfig)
        }
    } catch (_: ConfigException.Missing) {
        return autoConfig(clientId)
    }

    return autoConfig(clientId)
}

internal fun Application.kafkaConfig(clientId: String): InternalKafkaConfig = environment.config.kafkaConfig(clientId)

private fun autoConfig(clientId: String): InternalKafkaConfig =
    if (getRuntimeCluster() === RuntimeCluster.LOCAL) {
        logger.info("Kafka: In local runtime, using local Kafka config")
        InternalKafkaConfig.Local(clientId, bootstrapServers = System.getenv("BOOTSTRAP_SERVERS") ?: "localhost:9092")
    } else {
        logger.info("Kafka: In cloud runtime, using cloud Kafka config")

        val getEnv = { env: String ->
            System.getenv(env) ?: throw MissingNaisEnvException(env)
        }

        InternalKafkaConfig.Cloud(
            clientId,
            bootstrapServers = getEnv("KAFKA_BROKERS"),
            truststoreLocation = getEnv("KAFKA_TRUSTSTORE_PATH"),
            truststorePassword = getEnv("KAFKA_CREDSTORE_PASSWORD"),
            keystoreLocation = getEnv("KAFKA_KEYSTORE_PATH"),
            keystorePassword = getEnv("KAFKA_CREDSTORE_PASSWORD"),
        )
    }

private class MissingNaisEnvException(env: String) :
    IllegalStateException("Missing $env environment variable, are you running in nais?")
