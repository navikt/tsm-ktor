package no.nav.tsm.ktor.kafka.consumer

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.equals.shouldEqual
import io.ktor.server.application.*
import io.ktor.server.testing.*
import io.mockk.mockk
import io.mockk.verify
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.*
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import no.nav.tsm.ktor.kafka.test.KafkaContainer
import no.nav.tsm.ktor.kafka.test.send
import org.apache.kafka.clients.producer.KafkaProducer
import org.junit.jupiter.api.TestInstance

private data class Testy(
    val oogaId: String,
    val booga: Long,
)

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
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

    @Test
    fun `CancellationException from handleRecord should not stop the consumer job`() {
        val attempts = AtomicInteger(0)
        val retried = CompletableDeferred<Unit>()

        withConsumerJob(
            topic =
                onRecord<Testy>(
                    name = "testy-mc-testy-face",
                    onTombstone = {},
                    onRecord = { _ ->
                        if (attempts.incrementAndGet() == 2) retried.complete(Unit)
                        withTimeout(1.milliseconds) { delay(1.seconds) }
                    },
                )
        ) { _, running ->
            sendTestRecord()

            withTimeout(5.seconds) { retried.await() }
            running.isActive shouldEqual true
        }
    }

    @Test
    fun `cancelling the consumer job gracefully shuts down cafkaconsumer`() {
        val consumed = CompletableDeferred<Unit>()

        withConsumerJob(
            topic =
                onRecord<Testy>(
                    name = "testy-mc-testy-face",
                    onTombstone = {},
                    onRecord = { _ -> consumed.complete(Unit) },
                )
        ) { _, running ->
            sendTestRecord()
            withTimeout(5.seconds) { consumed.await() }

            running.cancel()

            withTimeout(5.seconds) { running.join() }
            running.isCancelled shouldEqual true

            eventually(5.seconds) { kafka.getGroupMembers("test-group-id") shouldEqual 0 }
        }
    }

    @Test
    fun `a record interrupted by shutdown is not committed and is redelivered, even with shouldSkip`() {
        val handling = CompletableDeferred<Unit>()

        withConsumerJob(
            topic =
                onRecord<Testy>(
                    name = "testy-mc-testy-face",
                    onTombstone = {},
                    onRecord = { _ ->
                        handling.complete(Unit)
                        delay(30.seconds)
                    },
                    shouldSkip = { true },
                )
        ) { _, running ->
            sendTestRecord()
            withTimeout(5.seconds) { handling.await() }

            running.cancel()
            withTimeout(5.seconds) { running.join() }
        }

        val redelivered = CompletableDeferred<Testy>()
        withConsumerJob(
            topic =
                onRecord<Testy>(
                    name = "testy-mc-testy-face",
                    onTombstone = {},
                    onRecord = { record -> redelivered.complete(record) },
                )
        ) { _, _ ->
            withTimeout(10.seconds) { redelivered.await() } shouldEqual Testy("124", 69)
        }
    }

    @Test
    fun `stop interrupts a blocked poll and the job finishes without being cancelled`() {
        val consumed = CompletableDeferred<Unit>()
        val record = sendTestRecord()

        withConsumerJob(
            pollDuration = 60.seconds,
            topic =
                onRecord<Testy>(
                    name = "testy-mc-testy-face",
                    onTombstone = {},
                    onRecord = { _ -> consumed.complete(Unit) },
                ),
        ) { job, running ->
            withTimeout(5.seconds) { consumed.await() }
            eventually(10.seconds) {
                kafka.getOffset("testy-mc-testy-face", "test-group-id") shouldEqual (record.offset() + 1L)
            }

            job.stop()

            withTimeout(5.seconds) { running.join() }
            running.isCancelled shouldEqual false

            eventually(5.seconds) { kafka.getGroupMembers("test-group-id") shouldEqual 0 }
        }
    }

    @Test
    fun `stopping during a retry backoff does not wait the backoff out`() {
        val failed = CompletableDeferred<Unit>()

        withConsumerJob(
            retryDuration = 60.seconds,
            topic =
                onRecord<Testy>(
                    name = "testy-mc-testy-face",
                    onTombstone = {},
                    onRecord = { _ ->
                        failed.complete(Unit)
                        throw RuntimeException("Test: Failed to process record")
                    },
                ),
        ) { job, running ->
            sendTestRecord()
            withTimeout(5.seconds) { failed.await() }

            job.stop()

            withTimeout(5.seconds) { running.join() }
            running.isCancelled shouldEqual false
        }
    }

    @Test
    fun `cancelling during a retry backoff shuts the job down promptly`() {
        val failed = CompletableDeferred<Unit>()

        withConsumerJob(
            retryDuration = 60.seconds,
            topic =
                onRecord<Testy>(
                    name = "testy-mc-testy-face",
                    onTombstone = {},
                    onRecord = { _ ->
                        failed.complete(Unit)
                        throw RuntimeException("Test: Failed to process record")
                    },
                ),
        ) { _, running ->
            sendTestRecord()
            withTimeout(5.seconds) { failed.await() }

            running.cancel()

            withTimeout(5.seconds) { running.join() }
            running.isCancelled shouldEqual true
        }
    }

    @Test
    fun `batched - stopping while a batch is being handled finishes it and retries the interrupted commit`() {
        val handling = CompletableDeferred<Unit>()
        val handled = AtomicBoolean(false)

        sendTestRecord()
        sendTestRecord()
        val last = sendTestRecord()

        withConsumerJob(
            topic =
                onRecords<Testy>(
                    name = "testy-mc-testy-face",
                    onRecords = { records ->
                        if (records.any { it.second.offset == last.offset() }) {
                            handling.complete(Unit)
                            handled.set(true)
                        }
                    },
                )
        ) { job, running ->
            withTimeout(5.seconds) { handling.await() }
            job.stop()

            withTimeout(5.seconds) { running.join() }
            running.isCancelled shouldEqual false
            handled.get() shouldEqual true
            kafka.getOffset("testy-mc-testy-face", "test-group-id") shouldEqual (last.offset() + 1L)
        }
    }

    @Test
    fun `batched - a batch interrupted by shutdown is not committed and is redelivered in full`() {
        val handling = CompletableDeferred<Unit>()

        sendTestRecord()
        sendTestRecord()
        val last = sendTestRecord()

        withConsumerJob(
            topic =
                onRecords<Testy>(
                    name = "testy-mc-testy-face",
                    onRecords = { records ->
                        if (records.any { it.second.offset == last.offset() }) {
                            handling.complete(Unit)
                            delay(30.seconds)
                        }
                    },
                )
        ) { _, running ->
            withTimeout(5.seconds) { handling.await() }

            running.cancel()
            withTimeout(5.seconds) { running.join() }
        }

        val redelivered = CompletableDeferred<Unit>()
        withConsumerJob(
            topic =
                onRecords<Testy>(
                    name = "testy-mc-testy-face",
                    onRecords = { records ->
                        if (records.any { it.second.offset == last.offset() }) {
                            redelivered.complete(Unit)
                        }
                    },
                )
        ) { _, _ ->
            withTimeout(10.seconds) { redelivered.await() }
        }
    }

    private fun sendTestRecord() =
        producer.send(
            topic = "testy-mc-testy-face",
            key = "test-key",
            value = """{"oogaId":"124","booga":69}""".toByteArray(),
        )

    /**
     * Runs a [KafkaConsumerJob] created with [createConsumer], detached from the Ktor lifecycle, so tests can stop or
     * cancel it themselves.
     */
    private fun withConsumerJob(
        topic: KafkaTopic<*>,
        pollDuration: Duration = 1.seconds,
        retryDuration: Duration = 10.milliseconds,
        block: suspend (job: KafkaConsumerJob, running: Job) -> Unit,
    ) = testApplication {
        kafka.configureKafka(this)

        val job =
            with(application) {
                install(KafkaConfig) {
                    clientId = "test-client-id"
                }

                createConsumer(
                    groupId = "test-group-id",
                    topic = topic,
                    pollDuration = pollDuration,
                    retryDuration = retryDuration,
                )
            }

        startApplication()

        val running = application.launch { job.start() }

        try {
            block(job, running)
        } finally {
            job.stop()
            withTimeoutOrNull(5.seconds) { running.join() } ?: running.cancel()
        }
    }
}
