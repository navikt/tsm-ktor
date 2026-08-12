package no.nav.tsm.ktor.kafka.test

import com.typesafe.config.ConfigFactory
import io.ktor.server.config.*
import io.ktor.server.testing.*
import java.util.*
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.kafka.ConfluentKafkaContainer

open class WithKafkaContainer(val topics: List<String>) {
    private val kafka: ConfluentKafkaContainer = ConfluentKafkaContainer("confluentinc/cp-kafka:8.1.0")

    init {
        kafka.start()
        createTopics(topics)
    }

    fun resetTopics() {
        createAdmin().deleteTopics(topics).all().get()
        createTopics(topics)
    }

    fun createTestProducer(): KafkaProducer<String, ByteArray> {
        val props =
            Properties().apply {
                this["bootstrap.servers"] = kafka.bootstrapServers
            }
        return KafkaProducer(props, StringSerializer(), ByteArraySerializer())
    }

    fun TestApplicationBuilder.initKafkaConfig() {
        environment {
            config = configWithKafka()
        }
    }

    fun configWithKafka(): HoconApplicationConfig {
        val hocon =
            """
                |kafka.config {
                |  "bootstrap.servers" = "${kafka.bootstrapServers}"
                |  "security.protocol" = "PLAINTEXT"
                |}
                """
                .trimMargin()
        return HoconApplicationConfig(ConfigFactory.parseString(hocon))
    }

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

    private fun createAdmin(): AdminClient {
        val props =
            Properties().apply {
                this[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
            }
        return AdminClient.create(props)
    }

    private fun createTopics(topics: List<String>) {
        val admin = createAdmin()
        admin.createTopics(topics.map { NewTopic(it, 1, 1) }).all().get()
    }
}
