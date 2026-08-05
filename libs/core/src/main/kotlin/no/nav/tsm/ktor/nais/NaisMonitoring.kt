package no.nav.tsm.ktor.nais

import dev.hayden.Check
import dev.hayden.CheckBuilder
import dev.hayden.KHealth
import io.ktor.server.application.*
import io.ktor.server.metrics.micrometer.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

class NaisMonitoringPluginConfig {
    internal var healthChecks = linkedSetOf<Check>()
    internal var readyChecks = linkedSetOf<Check>()

    fun alive(init: CheckBuilder.() -> Unit) {
        healthChecks = CheckBuilder().apply(init).checks
    }

    fun ready(init: CheckBuilder.() -> Unit) {
        readyChecks = CheckBuilder().apply(init).checks
    }
}

val NaisMonitoring =
    createApplicationPlugin("NaisMonitoring", ::NaisMonitoringPluginConfig) {
        installMetrics()
        installHealthChecks()
    }

private fun PluginBuilder<NaisMonitoringPluginConfig>.installHealthChecks() {
    application.install(KHealth) {
        healthChecks {
            healthCheckPath = "/internal/health/alive"

            pluginConfig.healthChecks.forEach {
                check(it.checkName, it.check)
            }
        }

        readyChecks {
            readyCheckPath = "/internal/health/ready"

            pluginConfig.readyChecks.forEach {
                check(it.checkName, it.check)
            }
        }
    }
}

private fun PluginBuilder<NaisMonitoringPluginConfig>.installMetrics() {
    val appRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    application.install(MicrometerMetrics) { registry = appRegistry }
    application.routing { get("/internal/metrics") { call.respond(appRegistry.scrape()) } }
}
