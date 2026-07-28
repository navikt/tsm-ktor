package no.nav.tsm.ktor.auth.entra

import io.ktor.server.auth.AuthenticationStrategy
import io.ktor.server.auth.authenticate
import io.ktor.server.routing.Route

public fun Route.entraMachineToken(
    build: Route.() -> Unit
): Route {
    return authenticate(
        configurations = arrayOf(ENTRA_MACHINE_TOKEN),
        strategy = AuthenticationStrategy.Required,
        build = build
    )
}
