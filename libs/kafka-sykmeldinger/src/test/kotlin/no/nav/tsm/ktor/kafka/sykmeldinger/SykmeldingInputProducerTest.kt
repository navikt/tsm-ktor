package no.nav.tsm.ktor.kafka.sykmeldinger

import com.typesafe.config.ConfigFactory
import io.kotest.matchers.equals.shouldEqual
import io.ktor.server.application.install
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.testing.TestApplicationBuilder
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import no.nav.tsm.ktor.kafka.producer.KafkaProducer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.testcontainers.kafka.ConfluentKafkaContainer

class SykmeldingInputProducerTest {
    private val kafka: ConfluentKafkaContainer =
        ConfluentKafkaContainer("confluentinc/cp-kafka:8.1.0").apply {
            start()

            val admin = AdminClient.create(mapOf("bootstrap.servers" to bootstrapServers))
            admin.createTopics(listOf(NewTopic("tsm.sykmeldinger-input", 1, 1))).all().get()
        }

    @Test
    fun `should configure a producer and connect`() = testApplication {
        initKafkaConfig(kafka)

        application.install(KafkaProducer) {
            clientId = "producer-tester"
        }

        val producer = application.sykmeldingInputProducer()
        val meta = producer.tombstone("mordi")

        meta.topic() shouldEqual "tsm.sykmeldinger-input"
    }
}

fun TestApplicationBuilder.initKafkaConfig(kafka: ConfluentKafkaContainer) {
    val hocon =
        """
            |kafka.config {
            |  "bootstrap.servers" = "${kafka.bootstrapServers}"
            |  "security.protocol" = "PLAINTEXT"
            |}
            """
            .trimMargin()

    environment {
        config = HoconApplicationConfig(ConfigFactory.parseString(hocon))
    }
}
