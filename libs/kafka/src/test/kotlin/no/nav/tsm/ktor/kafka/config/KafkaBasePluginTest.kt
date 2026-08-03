package no.nav.tsm.ktor.kafka.config

import com.typesafe.config.ConfigFactory
import io.kotest.matchers.equals.shouldEqual
import io.ktor.server.application.install
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.plugins.di.dependencies
import io.ktor.server.testing.testApplication
import kotlin.test.Test

class KafkaBasePluginTest {

    @Test
    fun `should be installable`() = testApplication {
        application.install(KafkaConfig) {
            clientId = "test-client"
        }

        val config: InternalKafkaConfig by application.dependencies

        config.clientId shouldEqual "test-client"
        config.toProperties()["bootstrap.servers"] shouldEqual "localhost:9092"
    }

    @Test
    fun `should be installable with hocon overrides`() = testApplication {
        environment {
            config =
                HoconApplicationConfig(
                    ConfigFactory.parseMap(
                            mapOf(
                                // Nais injected values
                                "KAFKA_BROKERS" to "kafka-1:9092,kafka-2:9092",
                                "KAFKA_TRUSTSTORE_PATH" to "/var/run/secrets/kafka/truststore.jks",
                                "KAFKA_CREDSTORE_PASSWORD" to "credstore-password",
                                "KAFKA_KEYSTORE_PATH" to "/var/run/secrets/kafka/keystore.p12",
                            )
                        )
                        .withFallback(ConfigFactory.parseResources("application-kafka-test-full.conf"))
                        .resolve()
                )
        }

        application.install(KafkaConfig) {
            clientId = "test-client"
        }

        val config: InternalKafkaConfig by application.dependencies
        val properties = config.toProperties()

        config.clientId shouldEqual "test-client"
        properties["bootstrap.servers"] shouldEqual "kafka-1:9092,kafka-2:9092"
        properties["security.protocol"] shouldEqual "SSL"
        properties["ssl.truststore.location"] shouldEqual "/var/run/secrets/kafka/truststore.jks"
        properties["ssl.truststore.password"] shouldEqual "credstore-password"
        properties["ssl.truststore.type"] shouldEqual "jks"
        properties["ssl.keystore.location"] shouldEqual "/var/run/secrets/kafka/keystore.p12"
        properties["ssl.keystore.password"] shouldEqual "credstore-password"
        properties["ssl.keystore.type"] shouldEqual "PKCS12"
    }
}
