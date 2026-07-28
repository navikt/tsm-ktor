package no.nav.tsm.ktor.auth.entra

import io.ktor.server.application.createApplicationPlugin

val EntraAuth =
    createApplicationPlugin(name = "EntraAuth") { println("SimplePlugin is installed!") }
