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

/**
 * Enables both entra machine token and entra on-behalf-of token. Use call.onBehalfOfUserMaybe() to retrieve the user
 * principal if present, or null if the request was authenticated with a machine token.
 */
fun Route.entraBoth(build: Route.() -> Unit): Route {
    return authenticate(
        configurations = arrayOf(ENTRA_MACHINE_TOKEN, ENTRA_ON_BEHALF_OF),
        strategy = AuthenticationStrategy.FirstSuccessful,
        build = build,
    )
}
