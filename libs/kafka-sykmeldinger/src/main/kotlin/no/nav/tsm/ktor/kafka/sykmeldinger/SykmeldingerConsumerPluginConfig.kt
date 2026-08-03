package no.nav.tsm.ktor.kafka.sykmeldinger

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import no.nav.tsm.ktor.kafka.consumer.RecordMeta
import no.nav.tsm.sykmelding.input.core.model.SykmeldingRecord

class SykmeldingConsumerPluginConfig {
    lateinit var groupId: String
    lateinit var onRecord: suspend (SykmeldingRecord) -> Unit
    lateinit var onTombstone: suspend (meta: RecordMeta) -> Unit

    var pollDuration: Duration = 10.seconds
    var retryDuration: Duration = 60.seconds
}
