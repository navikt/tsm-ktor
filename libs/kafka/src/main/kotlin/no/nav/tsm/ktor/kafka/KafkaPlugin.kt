package no.nav.tsm.ktor.kafka

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import io.ktor.server.application.*
import io.ktor.server.application.hooks.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import no.nav.tsm.ktor.logger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

@DslMarker annotation class KafkaConsumerDsl

@KafkaConsumerDsl
class KafkaConsumerPluginConfig {
    lateinit var groupId: String
    var pollDuration: Duration = 10.seconds
    var retryDuration: Duration = 60.seconds
    val topics: MutableList<KafkaTopic<*>> = mutableListOf()

    inline fun <reified RecordType : Any> topic(
        name: String,
        noinline onRecord: (RecordType?) -> Unit,
        noinline onTombstone: (key: String) -> Unit,
    ) {
        topics +=
            KafkaTopic(
                topic = name,
                onRecord = onRecord,
                onTombstone = onTombstone,
                jacksonRef = jacksonTypeRef<RecordType>(),
            )
    }
}

class KafkaTopic<RecordType : Any>(
    val topic: String,
    val onRecord: (record: RecordType?) -> Unit,
    val onTombstone: (key: String) -> Unit,
    val jacksonRef: TypeReference<RecordType>,
) {
    fun parse(value: ByteArray?): () -> Unit {
        val parsed = value?.let { kafkaObjectMapper.readValue(it, jacksonRef) }

        return { onRecord(parsed) }
    }
}

val KafkaConsumer =
    createApplicationPlugin(name = "KafkaConsumer", ::KafkaConsumerPluginConfig) {
        val logger = logger()
        val configuredTopics: List<String> = pluginConfig.topics.map { it.topic }
        val kafkaConfig =
            application.kafkaConfig().toProperties().apply {
                this[ConsumerConfig.GROUP_ID_CONFIG] = pluginConfig.groupId
                this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
                this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
            }

        val consumer: org.apache.kafka.clients.consumer.KafkaConsumer<String, ByteArray?> =
            org.apache.kafka.clients.consumer.KafkaConsumer(kafkaConfig, StringDeserializer(), ByteArrayDeserializer())

        on(MonitoringEvent(ApplicationStarted)) { application ->
            logger.info("Server is started")
            application.launch {
                withContext(Dispatchers.IO) {
                    while (isActive) {
                        logger.debug("Subscribing to topics: ${configuredTopics.joinToString(", ")}")
                        consumer.subscribe(configuredTopics)
                        try {
                            while (isActive) {
                                println("pollin")
                                val records = consumer.poll(pluginConfig.pollDuration.toJavaDuration())

                                if (records.isEmpty) {
                                    logger.debug(
                                        "Got no records after ${pluginConfig.pollDuration}, continuing to poll"
                                    )
                                    continue
                                }

                                for (record in records) {
                                    val topic = record.topic()
                                    val handler = pluginConfig.topics.find { it.topic == topic }
                                    requireNotNull(handler) {
                                        "Topic $topic was subscribed, but found no configuration with onRecord for it."
                                    }

                                    val value = record.value()
                                    if (value == null) {
                                        logger.debug("Received tombstone for key ${record.key()} on topic $topic")
                                        handler.onTombstone(record.key())
                                        val offsets =
                                            mapOf(
                                                TopicPartition(topic, record.partition()) to
                                                    OffsetAndMetadata(record.offset())
                                            )

                                        println("Committing ${offsets}")
                                        consumer.commitSync(offsets)
                                        continue
                                    }

                                    println("Received record on ${record.topic()}")

                                    println(record.value()?.decodeToString())
                                    val deliver = handler.parse(record.value())
                                    deliver()
                                    val offsets =
                                        mapOf(
                                            TopicPartition(topic, record.partition()) to
                                                OffsetAndMetadata(record.offset())
                                        )

                                    println("Committing ${offsets}")
                                    consumer.commitSync(offsets)
                                }
                            }
                        } catch (ex: CancellationException) {
                            logger.info("Kafka consumer cancelled gracefully (application stopping)", ex)
                        } catch (ex: Exception) {
                            logger.error("Error running Kafka consumer, waiting 60 seconds to retry", ex)
                            consumer.unsubscribe()
                            delay(pluginConfig.retryDuration)
                        }
                    }
                }
            }
        }

        on(MonitoringEvent(ApplicationStopped)) { application ->
            logger.info("Server is stopped")
            consumer.unsubscribe()
        }
    }
