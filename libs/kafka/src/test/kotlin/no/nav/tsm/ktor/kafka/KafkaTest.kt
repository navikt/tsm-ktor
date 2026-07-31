package no.nav.tsm.ktor.kafka

import com.typesafe.config.ConfigFactory
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.testing.testApplication
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import java.util.Properties
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.use
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
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
        val kafka =
            ConfluentKafkaContainer("confluentinc/cp-kafka:8.1.0").apply {
                start()
                createTopics(listOf("example-topic"))
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

            topic<MyRecord>(
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
    }
}

private fun ConfluentKafkaContainer.createTopics(topics: List<String>) {
    val props =
        Properties().apply {
            this[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        }
    AdminClient.create(props).use { admin ->
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

suspend fun KafkaProducer<String, ByteArray>.send(topic: String, key: String, value: ByteArray?) {
    withContext(Dispatchers.IO) {
        this@send.send(ProducerRecord(topic, key, value)).get()
    }
}
