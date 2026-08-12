package no.nav.tsm.ktor.kafka.consumer

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.equals.shouldEqual
import io.ktor.server.testing.*
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifyOrder
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.*
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import no.nav.tsm.ktor.kafka.config.kafkaConfig
import no.nav.tsm.ktor.kafka.test.WithKafkaContainer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.junit.jupiter.api.TestInstance

private data class MyRecord(
    val sykmeldingId: String,
    val someOthervalue: String,
    val hasManyValues: Boolean,
)

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaTest : WithKafkaContainer(topics = listOf("example-topic", "other-topic")) {
    val producer = createTestProducer()

    @AfterTest
    fun cleanup() {
        resetTopics()
    }

    @Test
    fun `simple config and producer tests with records and tombstone`() = testApplication {
        initKafkaConfig()

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
            val exampleOffset = getOffset("example-topic", "test-group-id")
            exampleOffset shouldEqual lastRecord.offset() + 1L

            val otherOffset = getOffset("other-topic", "test-group-id")
            otherOffset shouldEqual lastOtherRecord.offset() + 1L
        }
    }

    @Test
    fun `a cancellation from inside onRecord is retried, no shutdown`(): Unit = runBlocking {
        val attempts = AtomicInteger(0)
        val retried = CompletableDeferred<Unit>()

        withConsumerJob(
            configure = {
                consume<MyRecord>(
                    name = "example-topic",
                    onTombstone = {},
                    onRecord = { _ ->
                        if (attempts.incrementAndGet() == 2) retried.complete(Unit)
                        withTimeout(1.milliseconds) { delay(1.seconds) }
                    },
                )
            }
        ) { _, running ->
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )

            withTimeout(5.seconds) { retried.await() }
            running.isActive shouldEqual true
        }
    }

    @Test
    fun `cancelling the consumer stops it and closes the consumer`(): Unit = runBlocking {
        val consumer = spyk(testConsumer(configWithKafka().kafkaConfig("test-client")))
        val consumed = CompletableDeferred<Unit>()

        withConsumerJob(
            consumer = consumer,
            configure = {
                consume<MyRecord>(
                    name = "example-topic",
                    onTombstone = {},
                    onRecord = { _ -> consumed.complete(Unit) },
                )
            },
        ) { _, running ->
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )
            withTimeout(5.seconds) { consumed.await() }

            running.cancel()

            withTimeout(5.seconds) { running.join() }
            running.isCancelled shouldEqual true
            verify { consumer.close(any()) }
        }
    }

    @Test
    fun `cancelling while a record is being handled does not run the record retry path`(): Unit = runBlocking {
        val consumer = spyk(testConsumer(configWithKafka().kafkaConfig("test-client")))
        val handling = CompletableDeferred<Unit>()

        withConsumerJob(
            consumer = consumer,
            configure = {
                consume<MyRecord>(
                    name = "example-topic",
                    onTombstone = {},
                    onRecord = { _ ->
                        handling.complete(Unit)
                        delay(30.seconds)
                    },
                )
            },
        ) { _, running ->
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )
            withTimeout(5.seconds) { handling.await() }

            running.cancel()
            withTimeout(5.seconds) { running.join() }

            verify(exactly = 0) { consumer.unsubscribe() }
        }
    }

    @Test
    fun `stop wakes up the consumer so start returns and the consumer is closed`(): Unit = runBlocking {
        val consumer = spyk(testConsumer(configWithKafka().kafkaConfig("test-client")))
        val consumed = CompletableDeferred<Unit>()

        val record =
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )

        withConsumerJob(
            consumer = consumer,
            pollDuration = 60.seconds,
            configure = {
                consume<MyRecord>(
                    name = "example-topic",
                    onTombstone = {},
                    onRecord = { _ -> consumed.complete(Unit) },
                )
            },
        ) { job, running ->
            withTimeout(5.seconds) { consumed.await() }
            eventually(10.seconds) {
                getOffset("example-topic", "test-group-id") shouldEqual (record.offset() + 1L)
            }

            job.stop()

            withTimeout(5.seconds) { running.join() }
            running.isCancelled shouldEqual false

            verify { consumer.wakeup() }
            verify { consumer.close(any()) }
        }
    }

    @Test
    fun `stopping the application finishes the record being handled and commits it`(): Unit = runBlocking {
        val handling = CompletableDeferred<Unit>()
        val handled = AtomicBoolean(false)

        val app = TestApplication {
            initKafkaConfig()

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
        }
        app.start()

        val record =
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )

        try {
            withTimeout(5.seconds) { handling.await() }
        } finally {
            app.stop()
        }
        handled.get() shouldEqual true
        getOffset("example-topic", "test-group-id") shouldEqual (record.offset() + 1L)
    }

    @Test
    fun `stopping during a retry backoff does not wait the backoff out`(): Unit = runBlocking {
        val consumer = spyk(testConsumer(configWithKafka().kafkaConfig("test-client")))
        val failed = CompletableDeferred<Unit>()

        withConsumerJob(
            consumer = consumer,
            configure = {
                retryDuration = 60.seconds
                consume<MyRecord>(
                    name = "example-topic",
                    onTombstone = {},
                    onRecord = { _ ->
                        failed.complete(Unit)
                        throw RuntimeException("Test: Failed to process record")
                    },
                )
            },
        ) { job, running ->
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )
            withTimeout(5.seconds) { failed.await() }
            delay(3.seconds)

            job.stop()

            withTimeout(5.seconds) { running.join() }
            running.isCancelled shouldEqual false
            verify { consumer.close(any()) }
        }
    }

    @Test
    fun `cancelling during a retry backoff shuts the consumer down and closes it`(): Unit = runBlocking {
        val consumer = spyk(testConsumer(configWithKafka().kafkaConfig("test-client")))
        val failed = CompletableDeferred<Unit>()

        withConsumerJob(
            consumer = consumer,
            configure = {
                retryDuration = 60.seconds
                consume<MyRecord>(
                    name = "example-topic",
                    onTombstone = {},
                    onRecord = { _ ->
                        failed.complete(Unit)
                        throw RuntimeException("Test: Failed to process record")
                    },
                )
            },
        ) { _, running ->
            producer.send(
                topic = "example-topic",
                key = "test-key",
                value = """{"sykmeldingId":"124","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
            )
            withTimeout(5.seconds) { failed.await() }
            running.cancel()

            withTimeout(5.seconds) { running.join() }
            verify { consumer.close(any()) }
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

    private fun testConsumer(kafkaConfig: KafkaConfig): ByteArrayConsumer =
        ByteArrayConsumer(
            clientId = "test-client-id",
            groupId = "test-group-id",
            config = kafkaConfig,
        )

    private suspend fun withConsumerJob(
        kafkaConfig: KafkaConfig = configWithKafka().kafkaConfig("test-consumer"),
        consumer: ByteArrayConsumer = testConsumer(kafkaConfig),
        pollDuration: Duration = 1.seconds,
        configure: KafkaConsumerPluginConfig.() -> Unit,
        block: suspend (job: KafkaConsumerJob, running: Job) -> Unit,
    ) = coroutineScope {
        val pluginConfig =
            KafkaConsumerPluginConfig().apply {
                clientId = "test-client-id"
                groupId = "test-group-id"
                this.pollDuration = pollDuration
                retryDuration = 10.milliseconds
                configure()
            }

        val job = KafkaConsumerJob(pluginConfig.topics.map { it.topic }, consumer, pluginConfig)
        val running = launch { job.start() }

        try {
            block(job, running)
        } finally {
            job.stop()
            withTimeoutOrNull(5.seconds) { running.join() } ?: running.cancel()
        }
    }
}

private fun KafkaProducer<String, ByteArray>.send(topic: String, key: String, value: ByteArray?): RecordMetadata {
    return this@send.send(ProducerRecord(topic, key, value)).get()
}
