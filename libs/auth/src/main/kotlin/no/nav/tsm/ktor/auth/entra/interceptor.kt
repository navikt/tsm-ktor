package no.nav.tsm.ktor.auth.entra

import io.ktor.server.auth.AuthenticationStrategy
import io.ktor.server.auth.authenticate
import io.ktor.server.routing.Route

fun Route.entraMachineToken(build: Route.() -> Unit): Route {
    return authenticate(
        configurations = arrayOf(ENTRA_MACHINE_TOKEN),
        strategy = AuthenticationStrategy.Required,
        build = build,
    )
}

fun Route.entraOnBehalfOf(build: Route.() -> Unit): Route {
    return authenticate(
        configurations = arrayOf(ENTRA_ON_BEHALF_OF),
        strategy = AuthenticationStrategy.Required,
        build = build,
    )
}
