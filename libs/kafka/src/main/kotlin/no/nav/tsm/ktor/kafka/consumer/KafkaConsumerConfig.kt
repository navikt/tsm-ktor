package no.nav.tsm.ktor.kafka.consumer

import tools.jackson.module.kotlin.jacksonTypeRef

inline fun <reified RecordType : Any> onRecord(
    name: String,
    noinline onRecord: suspend (RecordType) -> Unit,
    noinline onTombstone: suspend (RecordMeta) -> Unit,
    noinline shouldSkip: (suspend (RecordMeta) -> Boolean)? = null,
) =
    KafkaTopic.Record(
        topic = name,
        onRecord = onRecord,
        onTombstone = onTombstone,
        jacksonRef = jacksonTypeRef<RecordType>(),
        shouldSkip = shouldSkip,
    )

inline fun <reified RecordType : Any> onRecord(
    name: String,
    noinline onRecord: (RecordType, RecordMeta) -> Unit,
    noinline onTombstone: (RecordMeta) -> Unit,
    noinline shouldSkip: (suspend (RecordMeta) -> Boolean)? = null,
) =
    KafkaTopic.WithMeta(
        topic = name,
        onRecord = onRecord,
        onTombstone = onTombstone,
        jacksonRef = jacksonTypeRef<RecordType>(),
        shouldSkip = shouldSkip,
    )

inline fun <reified RecordType : Any> onRecords(
    name: String,
    noinline onRecords: suspend (value: List<Pair<RecordType?, RecordMeta>>) -> Unit,
) =
    KafkaTopic.Batched(
        topic = name,
        onRecords = onRecords,
        jacksonRef = jacksonTypeRef<RecordType>(),
    )
