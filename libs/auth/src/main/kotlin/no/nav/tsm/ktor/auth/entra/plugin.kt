package no.nav.tsm.ktor.auth.entra

import com.auth0.jwk.JwkProviderBuilder
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.auth.authentication
import io.ktor.server.auth.jwt.JWTPrincipal
import io.ktor.server.auth.jwt.jwt
import java.net.URI
import no.nav.tsm.ktor.logger
import no.nav.tsm.ktor.nais.RuntimeCluster
import no.nav.tsm.ktor.nais.getRuntimeCluster

private val logger = logger()

val EntraAuth =
    createApplicationPlugin(name = "EntraAuth", ::EntraAuthConfig) {
        val config = pluginConfig

        if (config.autoStub && getRuntimeCluster() == RuntimeCluster.LOCAL) {
            logger.warn(
                "EntraAuth autoStub enabled and detected LOCAL runtime. Stubbing entra authentication."
            )
            application.authentication { provider(ENTRA_MACHINE_TOKEN) { authenticate {} } }
            return@createApplicationPlugin
        }

        val jwkProvider = JwkProviderBuilder(URI(config.jwksUri).toURL()).build()

        application.authentication {
            jwt(ENTRA_MACHINE_TOKEN) {
                verifier(jwkProvider, config.issuer) { withAudience(config.audience) }
                validate { credentials -> JWTPrincipal(credentials.payload) }
            }
        }
    }

val StubbedEntraAuth =
    createApplicationPlugin(name = "StubbedEntraAuth") {
        val runtime = getRuntimeCluster()
        require(runtime == RuntimeCluster.LOCAL) {
            "You are trying to use the StubbedEntraAuth plugin in ${runtime}. That's _very_ illegal."
        }

        logger.warn("StubbedEntraAuth installed. Stubbing entra authentication.")

        application.authentication { provider(ENTRA_MACHINE_TOKEN) { authenticate {} } }
    }
