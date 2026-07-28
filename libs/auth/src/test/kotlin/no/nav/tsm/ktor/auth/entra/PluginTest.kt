package no.nav.tsm.ktor.auth.entra

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.equals.shouldEqual
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.server.auth.authenticate
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.testing.testApplication
import kotlin.test.Test

class PluginTest {

    @Test
    fun `test basic installation of plugin`() = testApplication {
        install(EntraAuth) {
            jwksUri = "http://example.com/jwks"
            issuer = "http://example.com/issuer"
            audience = "http://example.com/audience"
        }

        routing {
            authenticate(ENTRA_MACHINE_TOKEN) { get("/test") { call.respondText("Authenticated") } }
        }

        val client = createClient {}
        val response = client.get("/test")

        response.status shouldEqual HttpStatusCode.Unauthorized
    }

    @Test
    fun `providing no config should default to using nais envs`() = testApplication {
        shouldThrow<MissingNaisEnvException> {
            install(EntraAuth)
            startApplication()
        }
    }

    @Test
    fun `autoStub should automatically install stub`() = testApplication {
        install(EntraAuth) {
            autoStub = true
        }

        routing {
            authenticate(ENTRA_MACHINE_TOKEN) { get("/test") { call.respondText("Authenticated") } }
        }

        val client = createClient {}
        val response = client.get("/test")

        response.status shouldEqual HttpStatusCode.OK
    }

    @Test
    fun `StubbedEntraAuth plugin should stub`() = testApplication {
        install(StubbedEntraAuth)

        routing {
            authenticate(ENTRA_MACHINE_TOKEN) { get("/test") { call.respondText("Authenticated") } }
        }

        val client = createClient {}
        val response = client.get("/test")

        response.status shouldEqual HttpStatusCode.OK
    }

}
