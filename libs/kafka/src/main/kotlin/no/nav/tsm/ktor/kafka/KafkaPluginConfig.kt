package no.nav.tsm.ktor.kafka

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.ObjectMapper
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
    val jacksonModules: MutableList<Module> = mutableListOf()

    fun jacksonModule(vararg module: Module) {
        jacksonModules += module
    }

    inline fun <reified RecordType : Any> consume(
        name: String,
        noinline onRecord: suspend (RecordType) -> Unit,
        noinline onTombstone: suspend (key: String) -> Unit,
    ) {
        topics +=
            KafkaTopic.Record(
                topic = name,
                onRecord = onRecord,
                onTombstone = onTombstone,
                jacksonRef = jacksonTypeRef<RecordType>(),
            )
    }

    inline fun <reified RecordType : Any> consume(
        name: String,
        noinline onRecord: (value: RecordType, meta: RecordMeta) -> Unit,
        noinline onTombstone: (key: String) -> Unit,
    ) {
        topics +=
            KafkaTopic.WithMeta(
                topic = name,
                onRecord = onRecord,
                onTombstone = onTombstone,
                jacksonRef = jacksonTypeRef<RecordType>(),
            )
    }
}

sealed interface KafkaTopic<RecordType : Any> {
    val topic: String
    val onTombstone: suspend (key: String) -> Unit

    suspend fun handleRecord(value: ByteArray, meta: RecordMeta, objectMapper: ObjectMapper)

    class Record<RecordType : Any>(
        override val topic: String,
        val onRecord: suspend (record: RecordType) -> Unit,
        override val onTombstone: suspend (key: String) -> Unit,
        val jacksonRef: TypeReference<RecordType>,
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
        override val onTombstone: suspend (key: String) -> Unit,
        val jacksonRef: TypeReference<RecordType>,
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
