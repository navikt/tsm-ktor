package no.nav.tsm.ktor.kafka

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@DslMarker annotation class KafkaConsumerDsl

@KafkaConsumerDsl
class KafkaConsumerPluginConfig {
    lateinit var groupId: String
    var pollDuration: Duration = 10.seconds
    var retryDuration: Duration = 60.seconds
    val topics: MutableList<KafkaTopic<*>> = mutableListOf()

    inline fun <reified RecordType : Any> topic(
        name: String,
        noinline onRecord: (RecordType) -> Unit,
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
    val onRecord: (record: RecordType) -> Unit,
    val onTombstone: (key: String) -> Unit,
    val jacksonRef: TypeReference<RecordType>,
) {
    fun handleRecord(value: ByteArray) {
        val parsed =
            try {
                kafkaObjectMapper.readValue(value, jacksonRef)
            } catch (e: Exception) {
                throw KafkaParseException(topic, e)
            }

        try {
            onRecord(parsed)
        } catch (e: Exception) {
            throw KafkaHandlerException(topic, e)
        }
    }
}

internal class KafkaParseException(val topic: String, cause: Throwable? = null) : RuntimeException(cause)

internal class KafkaHandlerException(val topic: String, cause: Throwable? = null) : RuntimeException(cause)
