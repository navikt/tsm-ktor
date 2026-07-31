package no.nav.tsm.ktor.kafka

import com.typesafe.config.ConfigFactory
import io.kotest.matchers.equals.shouldEqual
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.types.shouldBeTypeOf
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import no.nav.tsm.ktor.kafka.config.kafkaConfig

class KafkaConfigTest {

    @Test
    fun `should load kafka config from configuration when provided`() = testApplication {
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

        application {
            val config = kafkaConfig()
            config.shouldBeTypeOf<KafkaConfig.Raw>()

            val kafkaConfig = config.toProperties()
            kafkaConfig.shouldNotBeNull()
            kafkaConfig["bootstrap.servers"] shouldEqual "kafka-1:9092,kafka-2:9092"
            kafkaConfig["security.protocol"] shouldEqual "SSL"
            kafkaConfig["ssl.truststore.location"] shouldEqual "/var/run/secrets/kafka/truststore.jks"
            kafkaConfig["ssl.truststore.password"] shouldEqual "credstore-password"
            kafkaConfig["ssl.truststore.type"] shouldEqual "jks"
            kafkaConfig["ssl.keystore.location"] shouldEqual "/var/run/secrets/kafka/keystore.p12"
            kafkaConfig["ssl.keystore.password"] shouldEqual "credstore-password"
            kafkaConfig["ssl.keystore.type"] shouldEqual "PKCS12"
        }
    }

    @Test
    fun `should auto load kafka config when no conf-file configuration`() = testApplication {
        application {
            val config = kafkaConfig()
            config.shouldBeTypeOf<KafkaConfig.Local>()

            val kafkaConfig = config.toProperties()
            kafkaConfig.shouldNotBeNull()
            kafkaConfig["bootstrap.servers"] shouldEqual "localhost:9092"
            kafkaConfig["security.protocol"] shouldEqual "PLAINTEXT"
        }
    }
}
