package no.nav.tsm.ktor.kafka.consumer

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import tools.jackson.core.type.TypeReference
import tools.jackson.databind.JacksonModule
import tools.jackson.databind.ObjectMapper

class KafkaConsumerPluginConfig {
    /** Used internally in kafka for tracing and logging, set it to the pod name. */
    lateinit var clientId: String

    /** Used to identify the consumer group this consumer belongs to. */
    lateinit var groupId: String
    var pollDuration: Duration = 10.seconds
    var retryDuration: Duration = 60.seconds
    var closeTimeout: Duration = 3.seconds // close timeout for kafka operations (commit and close) on shutdown
    var shutdownTimeout: Duration = 10.seconds // shutdown timeout for KafkaConsumerPlugin
    val topics: MutableList<KafkaTopic<*>> = mutableListOf()
    val jacksonModules: MutableList<JacksonModule> = mutableListOf()

    fun jacksonModule(vararg module: JacksonModule) {
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
        noinline onRecord: suspend (RecordType, RecordMeta) -> Unit,
        noinline onTombstone: suspend (RecordMeta) -> Unit,
        noinline shouldSkip: (suspend (RecordMeta) -> Boolean)? = null,
    ) {
        topics += onRecord(name, onRecord, onTombstone, shouldSkip)
    }

    inline fun <reified RecordType : Any> batched(
        name: String,
        noinline onRecords: suspend (value: List<Pair<RecordType?, RecordMeta>>) -> Unit,
    ) {
        topics += onRecords(name, onRecords)
    }
}

sealed interface KafkaTopic<RecordType : Any> {
    val topic: String

    interface Unbatched<RecordType : Any> : KafkaTopic<RecordType> {
        val onTombstone: suspend (RecordMeta) -> Unit
        val shouldSkip: (suspend (RecordMeta) -> Boolean)?

        suspend fun handleRecord(value: ByteArray, meta: RecordMeta, objectMapper: ObjectMapper)
    }

    class Record<RecordType : Any>(
        override val topic: String,
        val onRecord: suspend (RecordType) -> Unit,
        override val onTombstone: suspend (RecordMeta) -> Unit,
        val jacksonRef: TypeReference<RecordType>,
        override val shouldSkip: (suspend (RecordMeta) -> Boolean)?,
    ) : Unbatched<RecordType> {
        override suspend fun handleRecord(value: ByteArray, meta: RecordMeta, objectMapper: ObjectMapper) {
            val parsed: RecordType? =
                try {
                    objectMapper.readValue(value, jacksonRef)
                } catch (e: Exception) {
                    throw KafkaParseException(meta, e)
                }

            try {
                if (parsed == null) {
                    onTombstone(meta)
                } else {
                    onRecord(parsed)
                }
            } catch (e: Exception) {
                throw KafkaHandlerException(listOf(meta), e)
            }
        }
    }

    class WithMeta<RecordType : Any>(
        override val topic: String,
        val onRecord: suspend (value: RecordType, meta: RecordMeta) -> Unit,
        override val onTombstone: suspend (RecordMeta) -> Unit,
        val jacksonRef: TypeReference<RecordType>,
        override val shouldSkip: (suspend (RecordMeta) -> Boolean)?,
    ) : Unbatched<RecordType> {
        override suspend fun handleRecord(value: ByteArray, meta: RecordMeta, objectMapper: ObjectMapper) {
            val parsed: RecordType? =
                try {
                    objectMapper.readValue(value, jacksonRef)
                } catch (e: Exception) {
                    throw KafkaParseException(meta, e)
                }

            try {
                if (parsed == null) {
                    onTombstone(meta)
                } else {
                    onRecord(parsed, meta)
                }
            } catch (e: Exception) {
                throw KafkaHandlerException(listOf(meta), e)
            }
        }
    }

    class Batched<RecordType : Any>(
        override val topic: String,
        val onRecords: suspend (value: List<Pair<RecordType?, RecordMeta>>) -> Unit,
        val jacksonRef: TypeReference<RecordType>,
    ) : KafkaTopic<RecordType> {
        suspend fun handleRecords(records: List<Pair<ByteArray?, RecordMeta>>, objectMapper: ObjectMapper) {
            val parsedRecords = records.map { (value, meta) ->
                val parsed =
                    try {
                        value?.let { objectMapper.readValue(it, jacksonRef) }
                    } catch (e: Exception) {
                        throw KafkaParseException(meta, e)
                    }
                parsed to meta
            }

            try {
                onRecords(parsedRecords)
            } catch (e: Exception) {
                throw KafkaHandlerException(parsedRecords.map { it.second }, e)
            }
        }
    }
}

internal class KafkaParseException(val meta: RecordMeta, cause: Throwable? = null) : RuntimeException(cause)

internal class KafkaHandlerException(val meta: List<RecordMeta>, cause: Throwable? = null) : RuntimeException(cause)

internal fun RecordMeta.description(): String {
    return "topic: $topic, partition: $partition, offset: $offset, timestamp: $timestamp"
}
