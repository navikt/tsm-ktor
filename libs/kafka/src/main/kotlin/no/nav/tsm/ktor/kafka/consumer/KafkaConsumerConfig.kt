package no.nav.tsm.ktor.kafka.consumer

import com.fasterxml.jackson.module.kotlin.jacksonTypeRef

inline fun <reified RecordType : Any> onRecord(
    name: String,
    noinline onRecord: suspend (RecordType) -> Unit,
    noinline onTombstone: suspend (RecordMeta) -> Unit,
) =
    KafkaTopic.Record(
        topic = name,
        onRecord = onRecord,
        onTombstone = onTombstone,
        jacksonRef = jacksonTypeRef<RecordType>(),
    )

inline fun <reified RecordType : Any> onRecord(
    name: String,
    noinline onRecord: (RecordType, RecordMeta) -> Unit,
    noinline onTombstone: (RecordMeta) -> Unit,
) =
    KafkaTopic.WithMeta(
        topic = name,
        onRecord = onRecord,
        onTombstone = onTombstone,
        jacksonRef = jacksonTypeRef<RecordType>(),
    )
