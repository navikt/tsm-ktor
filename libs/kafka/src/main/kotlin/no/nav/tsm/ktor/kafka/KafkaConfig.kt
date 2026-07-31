package no.nav.tsm.ktor.kafka

import io.ktor.server.application.Application
import java.util.Properties
import no.nav.tsm.ktor.logger
import no.nav.tsm.ktor.nais.RuntimeCluster
import no.nav.tsm.ktor.nais.getRuntimeCluster

private val logger = logger()

sealed interface KafkaConfig {
    fun toProperties(): Properties

    class Local(val bootstrapServers: String = "localhost:9092") : KafkaConfig {
        override fun toProperties() =
            Properties().apply {
                put("bootstrap.servers", bootstrapServers)
                put("security.protocol", "PLAINTEXT")
            }
    }

    class Cloud(
        val bootstrapServers: String,
        val truststoreLocation: String,
        val truststorePassword: String,
        val keystoreLocation: String,
        val keystorePassword: String,
    ) : KafkaConfig {
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

    class Raw(val config: Map<String, Any?>) : KafkaConfig {
        override fun toProperties() = Properties().apply { putAll(config) }
    }
}

fun Application.kafkaConfig(): KafkaConfig {
    val confConfig = environment.config.config("kafka.config").toMap()
    if (confConfig.isNotEmpty()) {
        return KafkaConfig.Raw(confConfig)
    }

    return autoConfig()
}

private fun autoConfig(): KafkaConfig =
    if (getRuntimeCluster() === RuntimeCluster.LOCAL) {
        logger.info("Kafka: In local runtime, using local Kafka config")
        KafkaConfig.Local(bootstrapServers = System.getenv("BOOTSTRAP_SERVERS") ?: "localhost:9092")
    } else {
        logger.info("Kafka: In cloud runtime, using cloud Kafka config")

        val getEnv = { env: String ->
            System.getenv(env) ?: throw MissingNaisEnvException(env)
        }

        KafkaConfig.Cloud(
            bootstrapServers = getEnv("KAFKA_BROKERS"),
            truststoreLocation = getEnv("KAFKA_TRUSTSTORE_PATH"),
            truststorePassword = getEnv("KAFKA_CREDSTORE_PASSWORD"),
            keystoreLocation = getEnv("KAFKA_KEYSTORE_PATH"),
            keystorePassword = getEnv("KAFKA_CREDSTORE_PASSWORD"),
        )
    }

private class MissingNaisEnvException(env: String) :
    IllegalStateException("Missing $env environment variable, are you running in nais?")
