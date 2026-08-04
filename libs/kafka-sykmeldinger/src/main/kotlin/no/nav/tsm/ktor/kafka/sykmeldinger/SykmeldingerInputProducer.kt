package no.nav.tsm.ktor.kafka.sykmeldinger

import io.ktor.server.application.Application
import no.nav.tsm.ktor.kafka.producer.KafkaRecordProducer
import no.nav.tsm.ktor.kafka.producer.createProducer
import no.nav.tsm.ktor.nais.getRuntimeInfo
import no.nav.tsm.sykmelding.input.core.model.SykmeldingModule
import no.nav.tsm.sykmelding.input.core.model.SykmeldingRecord

private const val INPUT_TOPIC = "tsm.sykmeldinger-input"
private const val REQUIRED_HEADER_SOURCE_APP = "source-app"
private const val REQUIRED_HEADER_SOURCE_NAMESPACE = "source-namespace"

fun Application.sykmeldingInputProducer(): SykmeldingInputProducer {
    val producer =
        createProducer<SykmeldingRecord>(
            topic = INPUT_TOPIC,
            jacksonModules = listOf(SykmeldingModule()),
        )

    return SykmeldingInputProducer(producer = producer)
}

class SykmeldingInputProducer(private val producer: KafkaRecordProducer<SykmeldingRecord>) {
    val info = getRuntimeInfo()

    fun send(key: String, record: SykmeldingRecord) =
        producer.send(
            key,
            record,
            mapOf(
                REQUIRED_HEADER_SOURCE_APP to info.appName,
                REQUIRED_HEADER_SOURCE_NAMESPACE to info.appNamespace,
            ),
        )

    fun tombstone(key: String) =
        producer.tombstone(
            key,
            mapOf(
                REQUIRED_HEADER_SOURCE_APP to info.appName,
                REQUIRED_HEADER_SOURCE_NAMESPACE to info.appNamespace,
            ),
        )
}
