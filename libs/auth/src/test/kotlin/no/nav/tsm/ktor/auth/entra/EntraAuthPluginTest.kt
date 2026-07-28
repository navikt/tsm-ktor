package no.nav.tsm.ktor.auth.entra

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.matchers.equals.shouldEqual
import io.kotest.matchers.nulls.shouldNotBeNull
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import no.nav.tsm.ktor.auth.entra.obo.EntraOnBehalfOfUser
import no.nav.tsm.ktor.auth.entra.obo.onBehalfOfUser

class EntraAuthPluginTest {

    @Test
    fun `test basic installation of plugin (machine)`() = testApplication {
        install(EntraAuth) {
            machine = true
            jwksUri = "http://example.com/jwks"
            issuer = "http://example.com/issuer"
            audience = "http://example.com/audience"
        }

        routing { entraMachineToken { get("/test") { call.respondText("Authenticated") } } }

        val client = createClient {}
        val response = client.get("/test")

        response.status shouldEqual HttpStatusCode.Unauthorized
    }

    @Test
    fun `test basic installation of plugin (obo)`() = testApplication {
        install(EntraAuth) {
            obo = true
            jwksUri = "http://example.com/jwks"
            issuer = "http://example.com/issuer"
            audience = "http://example.com/audience"
        }

        routing { entraOnBehalfOf { get("/test") { call.respondText("Authenticated") } } }

        val client = createClient {}
        val response = client.get("/test")

        response.status shouldEqual HttpStatusCode.Unauthorized
    }

    @Test
    fun `providing no config should default to using nais envs`() = testApplication {
        shouldThrow<MissingNaisEnvException> {
            install(EntraAuth) { machine = true }
            startApplication()
        }
    }

    @Test
    fun `enabling no modes should warn with error`() = testApplication {
        shouldThrowMessage(
            "EntraAuth plugin requires at least one of machine or obo to be enabled."
        ) {
            install(EntraAuth)
            startApplication()
        }
    }

    @Test
    fun `autoStub should automatically install stub for both machine and obo`() = testApplication {
        install(EntraAuth) {
            machine = true
            obo = true
            autoStub = true
        }

        routing {
            entraMachineToken { get("/machine") { call.respondText("Authenticated machine") } }
            entraOnBehalfOf { get("/obo") { call.respondText("Authenticated OBO") } }
        }

        val client = createClient {}
        client.get("/machine").status shouldEqual HttpStatusCode.OK
        client.get("/obo").status shouldEqual HttpStatusCode.OK
    }

    @Test
    fun `principal should be available in obo routes`() = testApplication {
        install(EntraAuth) {
            machine = true
            obo = true
            autoStub = true
        }

        var principal: EntraOnBehalfOfUser? = null
        routing {
            entraOnBehalfOf {
                get("/obo") {
                    principal = call.onBehalfOfUser()

                    call.respondText("Authenticated OBO")
                }
            }
        }

        val client = createClient {}
        client.get("/obo").status shouldEqual HttpStatusCode.OK

        principal.shouldNotBeNull()
        principal.name shouldEqual "Stub User"
    }

    @Test
    fun `StubbedMachineAuth plugin should stub`() = testApplication {
        install(StubbedMachineAuth)

        routing { entraMachineToken { get("/test") { call.respondText("Authenticated") } } }

        val client = createClient {}
        val response = client.get("/test")

        response.status shouldEqual HttpStatusCode.OK
    }

    @Test
    fun `StubbedOnBehalfOfAuth plugin should stub`() = testApplication {
        install(StubbedOnBehalfOfAuth)

        routing { entraOnBehalfOf { get("/test") { call.respondText("Authenticated") } } }

        val client = createClient {}
        val response = client.get("/test")

        response.status shouldEqual HttpStatusCode.OK
    }
}
