package no.nav.tsm.ktor.kafka.producer

import io.ktor.server.application.*
import io.ktor.server.plugins.di.*
import io.ktor.server.testing.*
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.*
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import no.nav.tsm.ktor.kafka.config.kafkaConfig
import no.nav.tsm.ktor.kafka.consumer.ByteArrayConsumer
import no.nav.tsm.ktor.kafka.consumer.KafkaConsumer
import no.nav.tsm.ktor.kafka.consumer.RecordMeta
import no.nav.tsm.ktor.kafka.consumer.toRecordMeta
import no.nav.tsm.ktor.kafka.test.KafkaContainer
import org.junit.jupiter.api.Assertions.assertNull
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

private data class VeryCool(
    val sykmeldingId: String,
    val veryCool: Boolean,
)

class KafkaProducerPluginTest {
    val kafka = KafkaContainer(createTopics = listOf("example-topic", "other-topic"))

    @Test
    fun `test producer plugin should produce`() = testApplication {
        kafka.configureKafka(this)

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
        kafka.configureKafka(this)

        val mock = mockk<(VeryCool) -> Unit>(relaxed = true)
        application.installTestConsumer {
            mock(it)
        }

        startApplication()

        val producer = application.createProducer<VeryCool>("example-topic")
        producer.send("test-key", VeryCool("123", true))

        verify(timeout = 5000) { mock(VeryCool("123", true)) }
    }

    @Test
    fun `generics should work when provided through ktor di`() = testApplication {
        kafka.configureKafka(this)

        val mock = mockk<(VeryCool) -> Unit>(relaxed = true)
        application.installTestConsumer {
            mock(it)
        }

        application.dependencies {
            provide<KafkaRecordProducer<VeryCool>> {
                this@testApplication.application.createProducer("example-topic")
            }
        }

        startApplication()

        val producer: KafkaRecordProducer<VeryCool> by application.dependencies
        producer.send("test-key", VeryCool("123", true))

        verify(timeout = 5000) { mock(VeryCool("123", true)) }
    }

    @Test
    fun `test tombstone produces tombstone not 'null'`() = testApplication {
        kafka.configureKafka(this)
        install(KafkaConfig) {
            clientId = "test-client"
        }
        val onRecord = CompletableDeferred<Pair<RecordMeta, ByteArray?>>()

        application.dependencies {
            provide<KafkaRecordProducer<VeryCool>> {
                this@testApplication.application.createProducer("example-topic")
            }
        }

        startApplication()

        val producer: KafkaRecordProducer<VeryCool> by application.dependencies
        producer.tombstone("test-key")
        application.useByteArrayConsumer(onRecord)
        val record =
            withTimeoutOrNull(5000.milliseconds) {
                onRecord.await()
            }

        assertNotNull(record)
        assertNull(record.second)
    }
}

private fun Application.useByteArrayConsumer(onRecord: CompletableDeferred<Pair<RecordMeta, ByteArray?>>) {
    val consumer: ByteArrayConsumer =
        ByteArrayConsumer(
            "my-client-id",
            "my-group-id",
            environment.config.kafkaConfig("my-client-id"),
        )

    launch(Dispatchers.IO) {
        withTimeout(5.seconds) {
            consumer.subscribe(listOf("example-topic"))

            while (onRecord.isActive) {
                val records = consumer.poll(100.milliseconds.toJavaDuration())
                records.forEach { record ->
                    onRecord.complete(record.toRecordMeta() to record.value())
                }
                yield()
            }
        }
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
