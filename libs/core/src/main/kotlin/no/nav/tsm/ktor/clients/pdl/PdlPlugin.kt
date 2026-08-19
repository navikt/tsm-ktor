package no.nav.tsm.ktor.clients.pdl

import io.ktor.server.application.*
import no.nav.tsm.ktor.di.dynamicDependencies

/**
 * Ktor plugin for providing a PDL client implementation based on the environment (auto stub).
 *
 * In a cloud environment, it provides a `PdlCloudClient`, while in a local environment, it provides a `PdlLocalClient`.
 *
 * Note: This requires having access to 'tsm-pdl-cache' in the nais configuration-spec:
 * ```yaml
 *  accessPolicy:
 *    outbound:
 *     rules:
 *       - application: tsm-pdl-cache
 * ```
 */
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
