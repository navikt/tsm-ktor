package no.nav.tsm.ktor.clients.pdl

import io.ktor.server.application.*
import no.nav.tsm.ktor.di.dynamicDependencies

val PdlPlugin =
    createApplicationPlugin("PdlPlugin") {
        application.dynamicDependencies {
            cloud {
                provide<PdlClient>(PdlCloudClient::class)
            }
            local {
                provide<PdlClient>(PdlLocalClient::class)
            }
        }
    }
