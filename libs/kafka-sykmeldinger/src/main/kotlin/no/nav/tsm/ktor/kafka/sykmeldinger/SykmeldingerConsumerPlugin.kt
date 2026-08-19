package no.nav.tsm.ktor.kafka.sykmeldinger

import io.ktor.server.application.*
import no.nav.tsm.ktor.kafka.consumer.KafkaConsumer
import no.nav.tsm.ktor.logger
import no.nav.tsm.sykmelding.input.core.model.SykmeldingModule
import no.nav.tsm.sykmelding.input.core.model.SykmeldingRecord

/** Get records from tsm.sykmledinger in almost zero config! */
val SykmeldingerConsumer =
    createApplicationPlugin(name = "SymfoniSykmeldingerConsumer", ::SykmeldingConsumerPluginConfig) {
        val logger = logger()

        logger.info("Automagic configuration of tsm.sykmeldinge consumer enabled! \uD83D\uDE80")

        application.install(KafkaConsumer) {
            clientId = pluginConfig.clientId
            groupId = pluginConfig.groupId
            retryDuration = pluginConfig.retryDuration
            pollDuration = pluginConfig.pollDuration
            jacksonModule(SykmeldingModule())
            consume<SykmeldingRecord>(
                name = "tsm.sykmeldinger",
                onRecord = { record, meta -> pluginConfig.onRecord(record, meta) },
                onTombstone = { pluginConfig.onTombstone(it) },
            )
        }
    }
