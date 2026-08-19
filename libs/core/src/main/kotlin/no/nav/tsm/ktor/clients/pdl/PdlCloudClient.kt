package no.nav.tsm.ktor.clients.pdl

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.jackson3.*
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import no.nav.tsm.ktor.auth.texas.Texas
import no.nav.tsm.ktor.logger
import no.nav.tsm.ktor.otel.failSpan

class PdlCloudClient(
    httpClient: HttpClient,
    private val texasClient: Texas,
) : PdlClient {
    private val url = "http://tsm-pdl-cache"
    private val logger = logger()

    private val pdlHttpClient = httpClient.config {
        install(HttpRequestRetry) {
            retryOnServerErrors(maxRetries = 5)
            exponentialDelay()
        }
        install(ContentNegotiation) {
            jackson {}
        }
    }

    @WithSpan
    override suspend fun getPerson(ident: String): PdlPerson? {
        val (token) = getToken()

        val response =
            pdlHttpClient.get("$url/api/person") {
                headers {
                    append("Nav-Consumer-Id", "syk-inn-api")
                    append("Authorization", "Bearer $token")
                    append("Ident", ident)
                }
            }

        return when {
            response.status.isSuccess() ->
                try {
                    response.body<PdlPerson>()
                } catch (e: Exception) {
                    failSpan(Span.current(), e)
                    logger.error("Error deserializing PDL response", e)
                    throw PdlClient.UnknownError(e)
                }

            response.status == HttpStatusCode.NotFound -> null
            else -> {
                throw PdlClient.UnknownError(Exception("PDL request failed with status ${response.status}"))
            }
        }
    }

    override suspend fun getAktorId(ident: String): String? {
        val response = getPerson(ident) ?: return null

        return response.identer.firstOrNull { it.gruppe == PdlIdentgruppe.AKTORID && !it.historisk }?.ident
    }

    private suspend fun getToken() = texasClient.entraIdToken("tsm", "tsm-pdl-cache")
}
