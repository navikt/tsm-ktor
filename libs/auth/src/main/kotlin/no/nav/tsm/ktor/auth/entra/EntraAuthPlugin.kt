package no.nav.tsm.ktor.auth.entra

import com.auth0.jwk.JwkProviderBuilder
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.auth.authentication
import io.ktor.server.auth.jwt.JWTPrincipal
import io.ktor.server.auth.jwt.jwt
import java.net.URI
import no.nav.tsm.ktor.auth.entra.obo.stubPrincipal
import no.nav.tsm.ktor.auth.entra.obo.toEntraOnBehalfOfUser
import no.nav.tsm.ktor.logger
import no.nav.tsm.ktor.nais.RuntimeCluster
import no.nav.tsm.ktor.nais.getRuntimeCluster

private val logger = logger()

val EntraAuth =
    createApplicationPlugin(name = "EntraAuth", ::EntraAuthConfig) {
        val config = pluginConfig

        if (!config.machine && !config.obo) {
            throw IllegalArgumentException("EntraAuth plugin requires at least one of machine or obo to be enabled.")
        }

        if (config.autoStub && getRuntimeCluster() == RuntimeCluster.LOCAL) {
            if (config.machine) {
                logger.warn(
                    "EntraAuth(machine) autoStub enabled and detected LOCAL runtime. Stubbing Entra machine token authentication."
                )
                application.authentication { provider(ENTRA_MACHINE_TOKEN) { authenticate {} } }
            }

            if (config.obo) {
                logger.warn(
                    "EntraAuth(obo) autoStub enabled and detected LOCAL runtime. Stubbing Entra on-behalf-of token authentication."
                )
                application.authentication {
                    provider(ENTRA_ON_BEHALF_OF) {
                        authenticate { ctx -> ctx.principal(stubPrincipal) }
                    }
                }
            }

            return@createApplicationPlugin
        }

        val jwkProvider = JwkProviderBuilder(URI(config.jwksUri).toURL()).build()

        if (config.machine) {
            application.authentication {
                jwt(ENTRA_MACHINE_TOKEN) {
                    verifier(jwkProvider, config.issuer) { withAudience(config.audience) }
                    validate { credentials -> JWTPrincipal(credentials.payload) }
                }
            }

            logger.info("EntraAuth(machine) installed. Entra machine token authentication enabled.")
        }

        if (config.obo) {
            application.authentication {
                jwt(ENTRA_ON_BEHALF_OF) {
                    verifier(jwkProvider, config.issuer) { withAudience(config.audience) }
                    validate { credential -> credential.toEntraOnBehalfOfUser() }
                }
            }

            logger.info("EntraAuth(obo) installed. Entra on-behalf-of token authentication enabled.")
        }
    }

val StubbedMachineAuth =
    createApplicationPlugin(name = "StubbedMachineEntraAuth") {
        val runtime = getRuntimeCluster()
        require(runtime == RuntimeCluster.LOCAL) {
            "You are trying to use the StubbedEntraAuth plugin in ${runtime}. That's _very_ illegal."
        }

        logger.warn("StubbedEntraAuth installed. Stubbing entra authentication.")

        application.authentication { provider(ENTRA_MACHINE_TOKEN) { authenticate {} } }
    }

val StubbedOnBehalfOfAuth =
    createApplicationPlugin(name = "StubbedOnBehalfOfEntraAuth") {
        val runtime = getRuntimeCluster()
        require(runtime == RuntimeCluster.LOCAL) {
            "You are trying to use the StubbedOnBehalfOfAuth plugin in ${runtime}. That's _very_ illegal."
        }

        logger.warn("StubbedOnBehalfOfAuth installed. Stubbing entra authentication.")

        application.authentication {
            provider(ENTRA_ON_BEHALF_OF) { authenticate { ctx -> ctx.principal(stubPrincipal) } }
        }
    }
