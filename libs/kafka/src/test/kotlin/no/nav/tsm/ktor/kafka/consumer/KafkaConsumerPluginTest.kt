package no.nav.tsm.ktor.kafka.consumer

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.equals.shouldEqual
import io.ktor.server.testing.*
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.*
import no.nav.tsm.ktor.kafka.test.KafkaContainer
import no.nav.tsm.ktor.kafka.test.send
import no.nav.tsm.ktor.kafka.test.yeet
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.RecordMetadata
import org.junit.jupiter.api.TestInstance

private data class MyRecord(
    val sykmeldingId: String,
    val someOthervalue: String,
    val hasManyValues: Boolean,
)

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaTest {
    val kafka = KafkaContainer(createTopics = listOf("example-topic", "other-topic"))
    val producer: KafkaProducer<String, ByteArray> = kafka.createAnythingProducer()

    @Test
    fun `simple config and producer tests with records and tombstone`() = testApplication {
        kafka.configureKafka(this)

        val tombstoneMock = mockk<(RecordMeta) -> Unit>(relaxed = true)
        val recordMock = mockk<(MyRecord) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            consume<MyRecord>(
                name = "example-topic",
                onTombstone = { meta ->
                    tombstoneMock(meta)
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
            tombstoneMock(match { it.key == "test-key" })
        }

        eventually(5.seconds) {
            val offset = kafka.getOffset("example-topic", "test-group-id")
            offset shouldEqual (lastRecord.offset() + 1L)
        }
    }

    @Test
    fun `when record fails and shouldSkip returns true, skip it (poison pill)`() = testApplication {
        kafka.configureKafka(this)

        val tombstoneMock = mockk<(RecordMeta) -> Unit>(relaxed = true)
        val recordMock = mockk<(MyRecord) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            consume<MyRecord>(
                name = "example-topic",
                onTombstone = { meta ->
                    tombstoneMock(meta)
                },
                onRecord = { record ->
                    recordMock(record)
                },
                shouldSkip = { meta ->
                    meta.key == "test-key-2"
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
            value = """{"sykmeldingId":"125","poop":"abc","hasManyValues":true}""".toByteArray(),
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
            tombstoneMock(match { it.key == "test-key" })
        }

        eventually(5.seconds) {
            val offset = kafka.getOffset("example-topic", "test-group-id")
            offset shouldEqual (lastRecord.offset() + 1L)
        }
    }

    @Test
    fun `when record parsing fails, should not commit and should retry`() = testApplication {
        kafka.configureKafka(this)

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

        val offset = kafka.getOffset("example-topic", "test-group-id")
        // Not +1
        offset shouldEqual (badRecord.offset())

        // Let other tests skip this bad record
        kafka.forceOffset("example-topic", "test-group-id", badRecord.offset() + 1)
    }

    @Test
    fun `when onRecord fails, should not commit and should retry`() = testApplication {
        kafka.configureKafka(this)

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

        val offset = kafka.getOffset("example-topic", "test-group-id")
        // Not +1
        offset shouldEqual (willFailRecord.offset())

        // Let other tests skip this bad record
        kafka.forceOffset("example-topic", "test-group-id", willFailRecord.offset() + 1)
    }

    @Test
    fun `batched - simple batched tests with records and tombstone`() = testApplication {
        kafka.configureKafka(this)

        val recordsMock = mockk<(List<MyRecord?>) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            batched<MyRecord>(
                name = "example-topic",
                onRecords = { records ->
                    recordsMock(records.map { it.first })
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

        verify(timeout = 5000) { recordsMock(any()) }
        verifyOrder {
            recordsMock(listOf(MyRecord("124", "abc", true), MyRecord("125", "abc", true), null))
        }

        eventually(5.seconds) {
            val offset = kafka.getOffset("example-topic", "test-group-id")
            offset shouldEqual (lastRecord.offset() + 1L)
        }
    }

    @Test
    fun `batched - when parsing fails, should not commit entire batch`() = testApplication {
        kafka.configureKafka(this)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            batched<MyRecord>(
                name = "example-topic",
                onRecords = {},
            )
        }

        startApplication()

        val firstRecord =
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )

        // Let this one record be processed, or else the test will fail when ran alone
        delay(1.seconds)

        producer.yeet(
            topic = "example-topic",
            key = "test-key",
            value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
        )
        producer.yeet(
            topic = "example-topic",
            key = "test-key-2",
            value = """{"sykmeldingId":"125","tull":"abc","hasManyValues":true}""".toByteArray(),
        )
        producer.yeet(
            topic = "example-topic",
            key = "test-key",
            value = null,
        )

        delay(2.seconds)

        eventually(5.seconds) {
            val offset = kafka.getOffset("example-topic", "test-group-id")
            offset shouldEqual (firstRecord.offset() + 1L)
        }
    }

    @Test
    fun `should support multiple topics`() = testApplication {
        kafka.configureKafka(this)

        val tombOneMock = mockk<(RecordMeta) -> Unit>(relaxed = true)
        val recordOneMock = mockk<(MyRecord) -> Unit>(relaxed = true)
        val tombTwoMock = mockk<(RecordMeta) -> Unit>(relaxed = true)
        val recordTwoMock = mockk<(MyRecord) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            consume<MyRecord>(
                name = "example-topic",
                onTombstone = { meta ->
                    tombOneMock(meta)
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
        verify(timeout = 5000) { tombOneMock(match { it.key == "test-key" }) }
        verify(timeout = 5000) { tombOneMock(match { it.key == "test-key" }) }

        eventually(5.seconds) {
            val exampleOffset = kafka.getOffset("example-topic", "test-group-id")
            exampleOffset shouldEqual lastRecord.offset() + 1L

            val otherOffset = kafka.getOffset("other-topic", "test-group-id")
            otherOffset shouldEqual lastOtherRecord.offset() + 1L
        }
    }

    @Test
    fun `should be able to install multiple Kafka consumers`() = testApplication {
        kafka.configureKafka(this)

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
        kafka.configureKafka(this)

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
            val offset = kafka.getOffset("example-topic", "test-group-id")
            offset shouldEqual (lastRecord.offset() + 1L)
        }
    }

    @Test
    fun `records with byte array null should correctly invoke onTombstone`() = testApplication {
        kafka.configureKafka(this)

        val tombstoneMock = mockk<(RecordMeta) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
            clientId = "test-client-id"
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            consume<MyRecord>(
                name = "example-topic",
                onTombstone = {
                    tombstoneMock(it)
                },
                onRecord = {},
            )
        }

        startApplication()

        val lastRecord =
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = "null".toByteArray(),
            )

        verify(timeout = 5000) {
            tombstoneMock(
                match { it.key == "test-key" && it.topic == "example-topic" && it.partition == 0 && it.offset >= 0L }
            )
        }

        eventually(5.seconds) {
            val offset = kafka.getOffset("example-topic", "test-group-id")
            offset shouldEqual (lastRecord.offset() + 1L)
        }
    }

    @Test
    fun `stopping the application finishes the record being handled and commits it`() {
        val handling = CompletableDeferred<Unit>()
        val handled = AtomicBoolean(false)
        lateinit var record: RecordMetadata

        testApplication {
            kafka.configureKafka(this)

            install(KafkaConsumer) {
                clientId = "test-client-id"
                groupId = "test-group-id"
                pollDuration = 1.seconds
                retryDuration = 1.seconds

                consume<MyRecord>(
                    name = "example-topic",
                    onTombstone = {},
                    onRecord = { _ ->
                        handling.complete(Unit)
                        delay(2.seconds)
                        handled.set(true)
                    },
                )
            }

            startApplication()

            record =
                producer.send(
                    topic = "example-topic",
                    key = "test-key",
                    value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
                )

            withTimeout(5.seconds) { handling.await() }
            // testApplication stops the application here, mid-handling
        }

        handled.get() shouldEqual true
        kafka.getOffset("example-topic", "test-group-id") shouldEqual (record.offset() + 1L)
    }
}
