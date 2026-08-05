package no.nav.tsm.ktor.kafka.sykmeldinger

import io.kotest.matchers.equals.shouldEqual
import io.ktor.server.application.*
import io.ktor.server.testing.*
import kotlin.test.Test
import no.nav.tsm.ktor.kafka.producer.KafkaProducer
import no.nav.tsm.ktor.kafka.test.KafkaContainer

class SykmeldingInputProducerTest {
    private val kafka = KafkaContainer(createTopics = listOf("tsm.sykmeldinger-input"))

    @Test
    fun `should configure a producer and connect`() = testApplication {
        kafka.configureKafka(this)

        application.install(KafkaProducer) {
            clientId = "producer-tester"
        }

        val producer = application.sykmeldingInputProducer()
        val meta = producer.tombstone("mordi")

        meta.topic() shouldEqual "tsm.sykmeldinger-input"
    }
}
