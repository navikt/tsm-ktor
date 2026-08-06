package no.nav.tsm.ktor.auth.texas

import io.kotest.matchers.equals.shouldEqual
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.toByteArray
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.server.testing.testApplication
import io.ktor.utils.io.ByteReadChannel
import kotlin.test.Test
import kotlinx.coroutines.test.runTest
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.module.kotlin.jacksonMapperBuilder
import tools.jackson.module.kotlin.readValue

class TexasClientTest {
    private val defaultTexasConfig =
        TexasConfiguration(
            tokenEndpoint = "https://texas.example.com/token",
            introspectionEndpoint = "https://texas.example.com/introspect",
        )

    private val texasResponseMapper =
        jacksonMapperBuilder().propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build()

    @Test
    fun `should exchange token for correct target`() = runTest {
        val mockEngine = MockEngine { request ->
            request.url.toString() shouldEqual defaultTexasConfig.tokenEndpoint

            val payload = texasResponseMapper.readValue<Texas.TokenRequest>(request.body.toByteArray())

            payload.target shouldEqual "api://prod-gcp.tsm.tsm-pdl-cache/.default"
            payload.identityProvider shouldEqual "entra_id"

            respond(
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
                content = ByteReadChannel("""{"access_token":"ay.aeuheu","expires_in":3600,"token_type":"Bearer"}"""),
            )
        }

        val texas = Texas(httpClient = HttpClient(mockEngine) {}, config = defaultTexasConfig)

        val response = texas.entraIdToken("tsm", "tsm-pdl-cache", TexasTarget.PROD)
        response.token shouldEqual "ay.aeuheu"
    }

    @Test
    fun `should exchange token correctly for maskinporten`() = testApplication {
        val mockEngine = MockEngine { request ->
            request.url.toString() shouldEqual defaultTexasConfig.tokenEndpoint

            val payload = texasResponseMapper.readValue<Texas.TokenRequest>(request.body.toByteArray())

            payload.target shouldEqual "nhn:scoperino nhn:pepperino"
            payload.identityProvider shouldEqual "maskinporten"

            respond(
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
                content = ByteReadChannel("""{"access_token":"ay.aeuheu","expires_in":3600,"token_type":"Bearer"}"""),
            )
        }

        val texas = Texas(httpClient = HttpClient(mockEngine) {}, config = defaultTexasConfig)

        val response = texas.maskinporten("nhn:scoperino nhn:pepperino")
        response.token shouldEqual "ay.aeuheu"
    }
}
