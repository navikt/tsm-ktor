package no.nav.tsm.ktor.kafka.producer

import io.ktor.server.application.*
import io.ktor.server.testing.*
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds
import no.nav.tsm.ktor.kafka.consumer.KafkaConsumer
import no.nav.tsm.ktor.kafka.test.WithKafkaContainer

private data class VeryCool(
    val sykmeldingId: String,
    val veryCool: Boolean,
)

class KafkaProducerPluginTest : WithKafkaContainer(topics = listOf("example-topic", "other-topic")) {

    @Test
    fun `test producer plugin should produce`() = testApplication {
        initKafkaConfig()

        application.install(KafkaProducer) {
            clientId = "test-client"
        }

        val mock = mockk<(VeryCool) -> Unit>(relaxed = true)
        application.installTestConsumer { mock(it) }

        startApplication()

        val producer = application.createProducer<VeryCool>("example-topic")
        producer.send("test-key", VeryCool("123", true))

        verify(timeout = 5000) {
            mock(VeryCool("123", true))
        }
    }

    @Test
    fun `KafkaProducer plugin isn't needed if consumer already installed`() = testApplication {
        initKafkaConfig()

        val mock = mockk<(VeryCool) -> Unit>(relaxed = true)
        application.installTestConsumer {
            mock(it)
        }

        startApplication()

        val producer = application.createProducer<VeryCool>("example-topic")
        producer.send("test-key", VeryCool("123", true))

        verify(timeout = 5000) { mock(VeryCool("123", true)) }
    }
}

private fun Application.installTestConsumer(onRecord: suspend (VeryCool) -> Unit = {}) {
    install(KafkaConsumer) {
        clientId = "test-client-id"
        groupId = "test-group-id"
        pollDuration = 1.seconds
        retryDuration = 1.seconds

        consume<VeryCool>(
            name = "example-topic",
            onTombstone = {},
            onRecord = { record ->
                onRecord(record)
            },
        )
    }
}
