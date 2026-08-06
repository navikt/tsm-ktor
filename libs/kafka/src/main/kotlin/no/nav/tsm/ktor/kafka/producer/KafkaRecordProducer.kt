package no.nav.tsm.ktor.kafka.producer

import io.ktor.server.application.Application
import io.ktor.server.plugins.di.dependencies
import no.nav.tsm.ktor.kafka.config.InternalKafkaConfig
import no.nav.tsm.ktor.kafka.config.kafkaObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import tools.jackson.databind.JacksonModule

class KafkaRecordProducer<Payload>
private constructor(
    private val topic: String,
    config: InternalKafkaConfig,
    jacksonModules: List<JacksonModule> = emptyList(),
) {
    companion object {
        /** Automatically inject kafka config and initialize a KafkaRecordProducer */
        fun <Payload> initProducer(
            application: Application,
            topic: String,
            jacksonModules: List<JacksonModule> = emptyList(),
        ): KafkaRecordProducer<Payload> {
            val config: InternalKafkaConfig by application.dependencies

            return KafkaRecordProducer(
                topic = topic,
                config = config,
                jacksonModules = jacksonModules,
            )
        }
    }

    private val producer: KafkaProducer<String, Payload>
    private val objectMapper = kafkaObjectMapper.rebuild().addModules(jacksonModules).build()

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

    fun send(key: String, value: Payload, headers: Map<String, String> = emptyMap()): RecordMetadata {
        val record = ProducerRecord(topic, key, value)

        record.headers().apply {
            headers.forEach { (k, v) -> add(k, v.toByteArray()) }
        }

        return producer.send(record).get()
    }

    fun tombstone(key: String, headers: Map<String, String> = emptyMap()): RecordMetadata {
        val record = ProducerRecord<String, Payload>(topic, key, null)

        record.headers().apply {
            headers.forEach { (k, v) -> add(k, v.toByteArray()) }
        }

        return producer.send(record).get()
    }
}
