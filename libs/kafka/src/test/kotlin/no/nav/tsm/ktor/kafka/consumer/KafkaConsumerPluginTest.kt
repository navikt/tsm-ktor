package no.nav.tsm.ktor.kafka.consumer

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.equals.shouldEqual
import io.ktor.server.testing.*
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.delay
import no.nav.tsm.ktor.kafka.test.WithKafkaContainer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

private data class MyRecord(
    val sykmeldingId: String,
    val someOthervalue: String,
    val hasManyValues: Boolean,
)

class KafkaTest : WithKafkaContainer(topics = listOf("example-topic", "other-topic")) {
    val producer = createTestProducer()

    @Test
    fun `simple config and producer tests with records and tombstone`() = testApplication {
        initKafkaConfig()

        val tombstoneMock = mockk<(String) -> Unit>(relaxed = true)
        val recordMock = mockk<(MyRecord) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            consume<MyRecord>(
                name = "example-topic",
                onTombstone = { key ->
                    tombstoneMock(key)
                },
                onRecord = { record ->
                    recordMock(record)
                },
            )
        }

        startApplication()

        producer.send(
            topic = "example-topic",
            key = "test-key",
            value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
        )
        producer.send(
            topic = "example-topic",
            key = "test-key-2",
            value = """{"sykmeldingId":"125","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
        )
        val lastRecord =
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = null,
            )

        verify(timeout = 5000) { tombstoneMock(any()) }
        verifyOrder {
            recordMock(MyRecord("124", "abc", true))
            recordMock(MyRecord("125", "abc", true))
            tombstoneMock("test-key")
        }

        eventually(5.seconds) {
            val offset = getOffset("example-topic", "test-group-id")
            offset shouldEqual (lastRecord.offset() + 1L)
        }
    }

    @Test
    fun `when record parsing fails, should not commit and should retry`() = testApplication {
        initKafkaConfig()

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            consume<MyRecord>(
                name = "example-topic",
                onTombstone = {},
                onRecord = {},
            )
        }

        startApplication()

        producer.send(
            topic = "example-topic",
            key = "test-key",
            value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
        )
        val badRecord =
            producer.send(
                topic = "example-topic",
                key = "test-key-2",
                value = """{"someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )

        delay(2.seconds)

        val offset = getOffset("example-topic", "test-group-id")
        // Not +1
        offset shouldEqual (badRecord.offset())

        // Let other tests skip this bad record
        forceOffset("example-topic", "test-group-id", badRecord.offset() + 1)
    }

    @Test
    fun `when onRecord fails, should not commit and should retry`() = testApplication {
        initKafkaConfig()

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            var first = true
            consume<MyRecord>(
                name = "example-topic",
                onTombstone = {},
                onRecord = {
                    if (!first) {
                        throw RuntimeException("Test: Failed to process record")
                    }
                    first = false
                },
            )
        }

        startApplication()

        producer.send(
            topic = "example-topic",
            key = "test-key",
            value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
        )
        val willFailRecord =
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )

        delay(2.seconds)

        val offset = getOffset("example-topic", "test-group-id")
        // Not +1
        offset shouldEqual (willFailRecord.offset())

        // Let other tests skip this bad record
        forceOffset("example-topic", "test-group-id", willFailRecord.offset() + 1)
    }

    @Test
    fun `should support multiple topics`() = testApplication {
        initKafkaConfig()

        val tombOneMock = mockk<(String) -> Unit>(relaxed = true)
        val recordOneMock = mockk<(MyRecord) -> Unit>(relaxed = true)
        val tombTwoMock = mockk<(String) -> Unit>(relaxed = true)
        val recordTwoMock = mockk<(MyRecord) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            consume<MyRecord>(
                name = "example-topic",
                onTombstone = { key ->
                    tombOneMock(key)
                },
                onRecord = { record ->
                    recordOneMock(record)
                },
            )

            consume<MyRecord>(
                name = "other-topic",
                onTombstone = { key ->
                    tombTwoMock(key)
                },
                onRecord = { record ->
                    recordTwoMock(record)
                },
            )
        }

        startApplication()

        producer.send(
            topic = "example-topic",
            key = "test-key",
            value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
        )
        val lastRecord =
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = null,
            )
        producer.send(
            topic = "other-topic",
            key = "test-key",
            value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
        )
        val lastOtherRecord =
            producer.send(
                topic = "other-topic",
                key = "test-key",
                value = null,
            )

        verify(timeout = 5000) { recordOneMock(MyRecord("124", "abc", true)) }
        verify(timeout = 5000) { recordTwoMock(MyRecord("124", "abc", true)) }
        verify(timeout = 5000) { tombOneMock("test-key") }
        verify(timeout = 5000) { tombTwoMock("test-key") }

        eventually(5.seconds) {
            val exampleOffset = getOffset("example-topic", "test-group-id")
            exampleOffset shouldEqual lastRecord.offset() + 1L

            val otherOffset = getOffset("other-topic", "test-group-id")
            otherOffset shouldEqual lastOtherRecord.offset() + 1L
        }
    }

    @Test
    fun `should be able to install multiple Kafka consumers`() = testApplication {
        initKafkaConfig()

        val oneMock = mockk<(MyRecord) -> Unit>(relaxed = true)
        val twoMock = mockk<(MyRecord) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds
            consume<MyRecord>(
                name = "example-topic",
                onTombstone = {},
                onRecord = { record ->
                    oneMock(record)
                },
            )
        }

        install(KafkaConsumer) {
            clientId = "test-other-id"
            groupId = "test-group-other"
            pollDuration = 1.seconds
            retryDuration = 1.seconds
            consume<MyRecord>(
                name = "example-topic",
                onTombstone = {},
                onRecord = { record ->
                    twoMock(record)
                },
            )
        }

        startApplication()

        producer.send(
            topic = "example-topic",
            key = "test-key",
            value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
        )

        verify(timeout = 5000) { oneMock(any()) }
        verify(timeout = 5000) { twoMock(any()) }
    }

    @Test
    fun `consumeRecord should provide record metadata`() = testApplication {
        initKafkaConfig()

        val recordMock = mockk<(MyRecord) -> Unit>(relaxed = true)
        val metaMock = mockk<(RecordMeta) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            consume<MyRecord>(
                name = "example-topic",
                onTombstone = {},
                onRecord = { record -> recordMock(record) },
            )

            consume<MyRecord>(
                name = "example-topic",
                onTombstone = { _, _ -> },
                onRecord = { value, meta ->
                    recordMock(value)
                    metaMock(meta)
                },
            )
        }

        startApplication()

        val lastRecord =
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )

        verify(timeout = 5000) { recordMock(MyRecord("124", "abc", true)) }
        verify {
            metaMock(match { it.topic == "example-topic" && it.partition == 0 && it.offset >= 0L })
        }

        eventually(5.seconds) {
            val offset = getOffset("example-topic", "test-group-id")
            offset shouldEqual (lastRecord.offset() + 1L)
        }
    }
}

private fun KafkaProducer<String, ByteArray>.send(topic: String, key: String, value: ByteArray?): RecordMetadata {
    return this@send.send(ProducerRecord(topic, key, value)).get()
}
