package no.nav.tsm.ktor.kafka.config

import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinFeature
import tools.jackson.module.kotlin.KotlinModule

private val kotlinModule = KotlinModule.Builder().enable(KotlinFeature.StrictNullChecks).build()

internal val kafkaObjectMapper = JsonMapper.builder().addModules(kotlinModule).build()
