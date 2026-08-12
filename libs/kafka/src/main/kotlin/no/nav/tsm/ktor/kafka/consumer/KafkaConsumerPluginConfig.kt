package no.nav.tsm.ktor.kafka.consumer

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import no.nav.tsm.ktor.logger

private val logger = logger()

class KafkaConsumerPluginConfig {
    /** Used internally in kafka for tracing and logging, set it to the pod name. */
    lateinit var clientId: String
    /** Used to identify the consumer group this consumer belongs to. */
    lateinit var groupId: String
    var pollDuration: Duration = 10.seconds
    var retryDuration: Duration = 60.seconds
    var shutdownTimeout: Duration = 5.seconds
    var closeTimeout: Duration = 5.seconds
    val topics: MutableList<KafkaTopic<*>> = mutableListOf()
    val jacksonModules: MutableList<Module> = mutableListOf()

    fun jacksonModule(vararg module: Module) {
        jacksonModules += module
    }

    inline fun <reified RecordType : Any> consume(
        name: String,
        noinline onRecord: suspend (RecordType) -> Unit,
        noinline onTombstone: suspend (RecordMeta) -> Unit,
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
        noinline onRecord: (RecordType, RecordMeta) -> Unit,
        noinline onTombstone: (RecordMeta) -> Unit,
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
    val onTombstone: suspend (RecordMeta) -> Unit

    suspend fun handleRecord(value: ByteArray, meta: RecordMeta, objectMapper: ObjectMapper)

    class Record<RecordType : Any>(
        override val topic: String,
        val onRecord: suspend (RecordType) -> Unit,
        override val onTombstone: suspend (RecordMeta) -> Unit,
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
            } catch (e: CancellationException) {
                currentCoroutineContext().ensureActive()
                throw KafkaHandlerException(meta, e)
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
