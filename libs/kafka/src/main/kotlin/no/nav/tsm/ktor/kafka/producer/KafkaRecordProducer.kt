package no.nav.tsm.ktor.kafka.producer

import com.fasterxml.jackson.databind.Module
import no.nav.tsm.ktor.kafka.config.KafkaConfig
import no.nav.tsm.ktor.kafka.config.kafkaObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer

class KafkaRecordProducer<Payload>(
    private val topic: String,
    config: KafkaConfig,
    jacksonModules: List<Module> = emptyList(),
) {
    private val producer: KafkaProducer<String, Payload>
    private val objectMapper =
        kafkaObjectMapper().apply {
            jacksonModules.forEach { registerModule(it) }
        }

    init {
        class MessageSerializer : Serializer<Payload> {
            override fun serialize(topic: String, record: Payload): ByteArray? = objectMapper.writeValueAsBytes(record)
        }

        val kafkaProperties = config.toProperties()
        kafkaProperties.apply {
            this[ProducerConfig.CLIENT_ID_CONFIG] = config.clientId
            this[ProducerConfig.ACKS_CONFIG] = "all"
            this[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = "true"
            this[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "gzip"
        }

        producer = KafkaProducer(kafkaProperties, StringSerializer(), MessageSerializer())
    }

    fun send(key: String, value: Payload): RecordMetadata {
        val record = ProducerRecord(topic, key, value)
        return producer.send(record).get()
    }
}
