package no.nav.tsm.ktor.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers

class RecordMeta(
    val key: String,
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long,
    val headers: Headers,
)

fun ConsumerRecord<String, ByteArray?>.toRecordMeta(): RecordMeta =
    RecordMeta(
        key = key(),
        topic = topic(),
        partition = partition(),
        offset = offset(),
        timestamp = timestamp(),
        headers = headers(),
    )
