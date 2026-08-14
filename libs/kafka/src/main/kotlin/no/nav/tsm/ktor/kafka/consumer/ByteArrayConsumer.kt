package no.nav.tsm.ktor.kafka.consumer

import java.time.Duration
import no.nav.tsm.ktor.kafka.config.InternalKafkaConfig
import no.nav.tsm.ktor.logger
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

internal class ByteArrayConsumer(
    clientId: String,
    groupId: String,
    config: InternalKafkaConfig,
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

    fun commitSync(nextOffsets: Map<TopicPartition, OffsetAndMetadata>, timeout: Duration? = null) {
        logger.debug("Committing offsets $nextOffsets")
        if (timeout == null) {
            consumer.commitSync(nextOffsets)
        } else {
            consumer.commitSync(nextOffsets, timeout)
        }
    }

    fun wakeup() {
        consumer.wakeup()
    }

    fun close(timeout: Duration) {
        consumer.close(CloseOptions.timeout(timeout))
    }
}
