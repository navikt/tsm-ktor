package no.nav.tsm.ktor.kafka.test

import com.typesafe.config.ConfigFactory
import io.ktor.server.config.*
import io.ktor.server.testing.*
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.kafka.ConfluentKafkaContainer

class KafkaContainer(createTopics: List<String>, image: String = "confluentinc/cp-kafka:8.1.0") {
    val container: ConfluentKafkaContainer = ConfluentKafkaContainer(image)
    val config: Map<String, String>

    init {
        container.start()
        config = mapOf("bootstrap.servers" to container.bootstrapServers)
        createTopics(createTopics)
    }

    /**
     * Used early in your 'testApplication'-test to configure the Ktor application with the kafka-testcontainers
     * instance.
     */
    fun configureKafka(test: ApplicationTestBuilder) {
        val hocon =
            """
            |kafka.config {
            |  "bootstrap.servers" = "${container.bootstrapServers}"
            |  "security.protocol" = "PLAINTEXT"
            |}
            """
                .trimMargin()

        test.environment {
            config = HoconApplicationConfig(ConfigFactory.parseString(hocon))
        }
    }

    fun createAnythingProducer(): KafkaProducer<String, ByteArray> =
        KafkaProducer(config, StringSerializer(), ByteArraySerializer())

    fun getOffset(topic: String, groupId: String): Long {
        val admin = createAdmin()
        val offsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
        val partition =
            offsets.keys.find { it.topic() == topic }
                ?: throw IllegalStateException(
                    "Found no topic \"$topic\", only found ${offsets.keys.map { it.topic() }}"
                )
        return offsets[partition]?.offset() ?: throw IllegalStateException("Found no offset for topic \"$topic\"")
    }

    fun forceOffset(topic: String, groupId: String, offset: Long) {
        val admin = createAdmin()
        val partition = TopicPartition(topic, 0)
        admin.alterConsumerGroupOffsets(groupId, mapOf(partition to OffsetAndMetadata(offset))).all().get()
    }

    private fun createTopics(topics: List<String>) {
        val admin = createAdmin()
        admin.createTopics(topics.map { NewTopic(it, 1, 1) }).all().get()
    }

    private fun createAdmin(): AdminClient = AdminClient.create(config)
}

/** Sends a record and waits for the result. */
fun KafkaProducer<String, ByteArray>.send(topic: String, key: String, value: ByteArray?): RecordMetadata {
    return this.send(ProducerRecord(topic, key, value)).get()
}

/** Sends a record and does not wait for the result. */
fun KafkaProducer<String, ByteArray>.yeet(topic: String, key: String, value: ByteArray?) {
    this.send(ProducerRecord(topic, key, value))
}
