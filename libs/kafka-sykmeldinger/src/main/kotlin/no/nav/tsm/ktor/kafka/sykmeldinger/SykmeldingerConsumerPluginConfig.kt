package no.nav.tsm.ktor.kafka.sykmeldinger

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import no.nav.tsm.ktor.kafka.consumer.RecordMeta
import no.nav.tsm.sykmelding.input.core.model.SykmeldingRecord

class SykmeldingConsumerPluginConfig {
    /** Used internally in kafka for tracing and logging, set it to the pod name. */
    lateinit var clientId: String
    lateinit var groupId: String
    lateinit var onRecord: suspend (SykmeldingRecord, RecordMeta) -> Unit
    lateinit var onTombstone: suspend (meta: RecordMeta) -> Unit

    var pollDuration: Duration = 10.seconds
    var retryDuration: Duration = 60.seconds
}
