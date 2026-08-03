package no.nav.tsm.ktor.kafka.consumer

import java.time.Duration
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import no.nav.tsm.ktor.logger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

internal class ByteArrayConsumer(
    clientId: String,
    groupId: String,
    config: KafkaConfig,
) {
    private val logger = logger()
    private val consumer: KafkaConsumer<String, ByteArray?>

    init {
        val kafkaConfig =
            config.toProperties().apply {
                this[ConsumerConfig.CLIENT_ID_CONFIG] = clientId
                this[ConsumerConfig.GROUP_ID_CONFIG] = groupId
                this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
                this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
            }

        consumer = KafkaConsumer(kafkaConfig, StringDeserializer(), ByteArrayDeserializer())
    }

    fun subscribe(configuredTopics: List<String>) = consumer.subscribe(configuredTopics)

    fun unsubscribe() = consumer.unsubscribe()

    fun poll(timeout: Duration): ConsumerRecords<String, ByteArray?> = consumer.poll(timeout)

    fun commitSync(topic: String, record: ConsumerRecord<String, ByteArray?>) {
        logger.debug("Committing offset ${record.offset() + 1} for topic $topic, partition ${record.partition()}")
        this.commitSync(topic, record.partition(), record.offset() + 1)
    }

    private fun commitSync(topic: String, partition: Int, offset: Long) {
        consumer.commitSync(mapOf(TopicPartition(topic, partition) to OffsetAndMetadata(offset)))
    }
}
