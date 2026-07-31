package no.nav.tsm.ktor.kafka.sykmeldinger

import no.nav.tsm.sykmelding.input.core.model.SykmeldingRecord

class SykmeldingConsumerConfig {
    lateinit var groupId: String
    lateinit var onRecord: (SykmeldingRecord) -> Unit
    lateinit var onTombstone: (key: String) -> Unit
}
