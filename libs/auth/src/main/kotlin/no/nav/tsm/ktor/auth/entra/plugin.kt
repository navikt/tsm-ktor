package no.nav.tsm.ktor.auth.entra

import com.auth0.jwk.JwkProviderBuilder
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.auth.authentication
import io.ktor.server.auth.jwt.JWTPrincipal
import io.ktor.server.auth.jwt.jwt
import java.net.URI

val EntraAuth =
    createApplicationPlugin(name = "EntraAuth", ::EntraAuthConfig) {
        val config = pluginConfig
        val jwkProvider = JwkProviderBuilder(URI(config.jwksUri).toURL()).build()

        application.authentication {
            jwt(ENTRA_MACHINE_TOKEN) {
                verifier(jwkProvider, config.issuer) { withAudience(config.audience) }
                validate { credentials -> JWTPrincipal(credentials.payload) }
            }
        }
    }
