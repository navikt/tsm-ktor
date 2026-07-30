package no.nav.tsm.ktor.kafka

import com.typesafe.config.ConfigFactory
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.testing.testApplication
import java.util.Properties
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.use
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
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
    fun `dummy`() = testApplication {
        environment {
            config = HoconApplicationConfig(ConfigFactory.parseString(hocon))
        }

        install(KafkaConsumer) {
            groupId = "test-group-id"
            pollDuration = 1.seconds
            retryDuration = 1.seconds

            topic<MyRecord>(
                name = "example-topic",
                onTombstone = { key ->
                    println("Received tombstone for key: $key")
                },
                onRecord = { record ->
                    println("Received record: $record")
                },
            )
        }

        startApplication()

        println("Producing records to Kafka...")
        kafka.produce(
            topic = "example-topic",
            key = "test-key",
            value = """{"sykmeldingId":"123","someOthervalue":"abc","hasManyValues":true}""".toByteArray(),
        )
        kafka.produce(
            topic = "example-topic",
            key = "test-key-2",
            value = """{"garbage":"abc"}""".toByteArray(),
        )
        kafka.produce(
            topic = "example-topic",
            key = "test-key-2",
            value = null,
        )

        delay(5000)
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

suspend fun ConfluentKafkaContainer.produce(topic: String, key: String, value: ByteArray?) {
    withContext(Dispatchers.IO) {
        val props =
            Properties().apply {
                this["bootstrap.servers"] = bootstrapServers
            }
        KafkaProducer(props, StringSerializer(), ByteArraySerializer()).use { producer ->
            producer.send(ProducerRecord(topic, key, value)).get()
        }
    }
}
