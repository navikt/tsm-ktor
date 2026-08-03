package no.nav.tsm.ktor.kafka.consumer

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.ObjectMapper
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class KafkaConsumerPluginConfig {
    /** Used internally in kafka for tracing and logging, set it to the pod name. */
    lateinit var clientId: String
    /** Used to identify the consumer group this consumer belongs to. */
    lateinit var groupId: String
    var pollDuration: Duration = 10.seconds
    var retryDuration: Duration = 60.seconds
    val topics: MutableList<KafkaTopic<*>> = mutableListOf()
    val jacksonModules: MutableList<Module> = mutableListOf()

    fun jacksonModule(vararg module: Module) {
        jacksonModules += module
    }

    inline fun <reified RecordType : Any> consume(
        name: String,
        noinline onRecord: suspend (RecordType) -> Unit,
        noinline onTombstone: suspend (RecordMeta) -> Unit,
        noinline shouldSkip: (suspend (RecordMeta) -> Boolean)? = null,
    ) {
        topics += onRecord(name, onRecord, onTombstone, shouldSkip)
    }

    inline fun <reified RecordType : Any> consume(
        name: String,
        noinline onRecord: (RecordType, RecordMeta) -> Unit,
        noinline onTombstone: (RecordMeta) -> Unit,
        noinline shouldSkip: (suspend (RecordMeta) -> Boolean)? = null,
    ) {
        topics += onRecord(name, onRecord, onTombstone)
    }
}

sealed interface KafkaTopic<RecordType : Any> {
    val topic: String
    val onTombstone: suspend (RecordMeta) -> Unit
    val shouldSkip: (suspend (RecordMeta) -> Boolean)?

    suspend fun handleRecord(value: ByteArray, meta: RecordMeta, objectMapper: ObjectMapper)

    class Record<RecordType : Any>(
        override val topic: String,
        val onRecord: suspend (RecordType) -> Unit,
        override val onTombstone: suspend (RecordMeta) -> Unit,
        val jacksonRef: TypeReference<RecordType>,
        override val shouldSkip: (suspend (RecordMeta) -> Boolean)?,
    ) : KafkaTopic<RecordType> {
        override suspend fun handleRecord(value: ByteArray, meta: RecordMeta, objectMapper: ObjectMapper) {
            val parsed =
                try {
                    objectMapper.readValue(value, jacksonRef)
                } catch (e: Exception) {
                    throw KafkaParseException(meta, e)
                }

            try {
                onRecord(parsed)
            } catch (e: Exception) {
                throw KafkaHandlerException(meta, e)
            }
        }
    }

    class WithMeta<RecordType : Any>(
        override val topic: String,
        val onRecord: suspend (value: RecordType, meta: RecordMeta) -> Unit,
        override val onTombstone: suspend (RecordMeta) -> Unit,
        val jacksonRef: TypeReference<RecordType>,
        override val shouldSkip: (suspend (RecordMeta) -> Boolean)?,
    ) : KafkaTopic<RecordType> {
        override suspend fun handleRecord(value: ByteArray, meta: RecordMeta, objectMapper: ObjectMapper) {
            val parsed =
                try {
                    objectMapper.readValue(value, jacksonRef)
                } catch (e: Exception) {
                    throw KafkaParseException(meta, e)
                }

            try {
                onRecord(parsed, meta)
            } catch (e: Exception) {
                throw KafkaHandlerException(meta, e)
            }
        }
    }
}

internal class KafkaParseException(val meta: RecordMeta, cause: Throwable? = null) : RuntimeException(cause)

internal class KafkaHandlerException(val meta: RecordMeta, cause: Throwable? = null) : RuntimeException(cause)

internal fun RecordMeta.description(): String {
    return "topic: $topic, partition: $partition, offset: $offset, timestamp: $timestamp"
}
