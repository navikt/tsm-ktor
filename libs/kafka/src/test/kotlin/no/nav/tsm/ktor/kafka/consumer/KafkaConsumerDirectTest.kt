package no.nav.tsm.ktor.kafka.consumer

import io.ktor.server.application.*
import io.ktor.server.testing.*
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlinx.coroutines.launch
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import no.nav.tsm.ktor.kafka.test.KafkaContainer
import no.nav.tsm.ktor.kafka.test.send
import org.apache.kafka.clients.producer.KafkaProducer

private data class Testy(
    val oogaId: String,
    val booga: Long,
)

class KafkaConsumerDirectTest {
    val kafka = KafkaContainer(createTopics = listOf("testy-mc-testy-face"))
    val producer: KafkaProducer<String, ByteArray> = kafka.createAnythingProducer()

    @Test
    fun `should be able to simply create consumer and get raw access to the job`() = testApplication {
        kafka.configureKafka(this)

        val onRecordMock = mockk<(Testy) -> Unit>(relaxed = true)
        val job =
            with(application) {
                install(KafkaConfig) {
                    clientId = "test"
                }

                createConsumer(
                    groupId = "cool-app-consumer",
                    topic =
                        onRecord<Testy>(
                            name = "testy-mc-testy-face",
                            onTombstone = {},
                            onRecord = {
                                onRecordMock(it)
                            },
                        ),
                )
            }

        startApplication()

        application.launch {
            job.start()
        }

        producer.send(
            topic = "testy-mc-testy-face",
            key = "test-key",
            value = """{"oogaId":"124","booga":69}""".toByteArray(),
        )

        verify(timeout = 5000) { onRecordMock.invoke(Testy("124", 69)) }
    }
}
