package no.nav.tsm.ktor.kafka

import com.typesafe.config.ConfigFactory
import io.kotest.matchers.equals.shouldEqual
import io.ktor.server.config.*
import io.ktor.server.testing.*
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import java.util.*
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.kafka.ConfluentKafkaContainer

data class MyRecord(
    val sykmeldingId: String,
    val someOthervalue: String,
    val hasManyValues: Boolean,
)

class KafkaTest {
    companion object {
        val topics = listOf("example-topic", "other-topic")
        val kafka =
            ConfluentKafkaContainer("confluentinc/cp-kafka:8.1.0").apply {
                start()
                createTopics(topics)
            }

        val producer = createProducer(kafka.bootstrapServers)
        val hocon =
            """
                |kafka.config {
                |  "bootstrap.servers" = "${kafka.bootstrapServers}"
                |  "security.protocol" = "PLAINTEXT"
                |}
                """
                .trimMargin()
    }

    @Test
    fun `simple config and producer tests with records and tombstone`() = testApplication {
        environment {
            config = HoconApplicationConfig(ConfigFactory.parseString(hocon))
        }

        val tombstoneMock = mockk<(String) -> Unit>(relaxed = true)
        val recordMock = mockk<(MyRecord) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
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

        val offset = kafka.getOffset("example-topic", "test-group-id")
        offset shouldEqual (lastRecord.offset() + 1L)
    }

    @Test
    fun `should support multiple topics`() = testApplication {
        environment {
            config = HoconApplicationConfig(ConfigFactory.parseString(hocon))
        }

        val tombOneMock = mockk<(String) -> Unit>(relaxed = true)
        val recordOneMock = mockk<(MyRecord) -> Unit>(relaxed = true)
        val tombTwoMock = mockk<(String) -> Unit>(relaxed = true)
        val recordTwoMock = mockk<(MyRecord) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
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

        val exampleOffset = kafka.getOffset("example-topic", "test-group-id")
        exampleOffset shouldEqual lastRecord.offset() + 1L

        val otherOffset = kafka.getOffset("other-topic", "test-group-id")
        otherOffset shouldEqual lastOtherRecord.offset() + 1L
    }

    @Test
    fun `should be able to install multiple Kafka consumers`() = testApplication {
        environment {
            config = HoconApplicationConfig(ConfigFactory.parseString(hocon))
        }

        val oneMock = mockk<(MyRecord) -> Unit>(relaxed = true)
        val twoMock = mockk<(MyRecord) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
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
        environment {
            config = HoconApplicationConfig(ConfigFactory.parseString(hocon))
        }

        val recordMock = mockk<(MyRecord) -> Unit>(relaxed = true)
        val metaMock = mockk<(RecordMeta) -> Unit>(relaxed = true)

        install(KafkaConsumer) {
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

        val offset = kafka.getOffset("example-topic", "test-group-id")
        offset shouldEqual (lastRecord.offset() + 1L)
    }
}

private fun ConfluentKafkaContainer.createTopics(topics: List<String>) {
    createAdmin(bootstrapServers).use { admin ->
        admin.createTopics(topics.map { NewTopic(it, 1, 1) }).all().get()
    }
}

private fun createProducer(bootstrapServers: String): KafkaProducer<String, ByteArray> {
    val props =
        Properties().apply {
            this["bootstrap.servers"] = bootstrapServers
        }
    return KafkaProducer(props, StringSerializer(), ByteArraySerializer())
}

private fun KafkaProducer<String, ByteArray>.send(topic: String, key: String, value: ByteArray?): RecordMetadata {
    return this@send.send(ProducerRecord(topic, key, value)).get()
}

private fun createAdmin(bootstrapServers: String): AdminClient {
    val props =
        Properties().apply {
            this[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        }
    return AdminClient.create(props)
}

private fun ConfluentKafkaContainer.getOffset(topic: String, groupId: String): Long {
    val admin = createAdmin(bootstrapServers)
    val offsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
    val partition =
        offsets.keys.find { it.topic() == topic } ?: throw IllegalStateException("Found no topic \"$topic\"")
    return offsets[partition]?.offset() ?: throw IllegalStateException("Found no offset for topic \"$topic\"")
}
