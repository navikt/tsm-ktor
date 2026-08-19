package no.nav.tsm.ktor.kafka.sykmeldinger

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.equals.shouldEqual
import io.ktor.server.application.*
import io.ktor.server.testing.*
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds
import no.nav.tsm.ktor.kafka.test.KafkaContainer
import no.nav.tsm.ktor.kafka.test.send

class SykmeldingConsumerPluginTest {

    val topic = "tsm.sykmeldinger"
    val kafka = KafkaContainer(createTopics = listOf(topic))
    val producer = kafka.createAnythingProducer()

    @Test
    fun `should load the plugin`() = testApplication {
        kafka.configureKafka(this)

        application.install(SykmeldingerConsumer) {
            clientId = "test"
            groupId = "test"
            onRecord = { _, _ -> }
            onTombstone = {}
        }

        startApplication()

        val record = producer.send(topic = topic, key = "test", value = null)

        eventually(5.seconds) {
            val offset = kafka.getOffset(topic, "test")
            offset shouldEqual (record.offset() + 1L)
        }
    }
}
