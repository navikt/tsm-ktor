package no.nav.tsm.ktor.auth.texas

import com.fasterxml.jackson.annotation.JsonProperty
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.HttpRequestRetry
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import io.ktor.http.isTextType
import io.ktor.serialization.jackson.jackson
import io.opentelemetry.instrumentation.annotations.SpanAttribute
import io.opentelemetry.instrumentation.annotations.WithSpan
import no.nav.tsm.ktor.logger
import no.nav.tsm.ktor.nais.RuntimeCluster
import no.nav.tsm.ktor.nais.getRuntimeCluster

class TexasClient(
    httpClient: HttpClient,
    private val config: TexasConfiguration = TexasConfiguration(),
) {
    private val logger = logger()
    private val inferredCluster = getRuntimeCluster()

    private val texasHttpClient = httpClient.config {
        install(ContentNegotiation) { jackson {} }
        install(HttpRequestRetry) {
            retryOnServerErrors(maxRetries = 5)
            exponentialDelay()
        }
    }

    @WithSpan("Texas.naisToken")
    suspend fun entraIdToken(
        @SpanAttribute("namespace") namespace: String,
        @SpanAttribute("api") app: String,
        @SpanAttribute("cluster") cluster: RuntimeCluster = this.inferredCluster,
    ): TexasToken {
        val target = "api://${cluster.nais}.$namespace.$app/.default"
        val requestBody = TokenRequest(identityProvider = "entra_id", target = target)

        val response =
            texasHttpClient.post(config.tokenEndpoint) {
                contentType(ContentType.Application.Json)
                setBody(requestBody)
            }

        if (!response.status.isSuccess()) {
            response.logNonSuccess(target)
            throw IllegalStateException("Unable to request m2m token for: $target")
        }

        val body = response.body<TokenResponse>()
        return TexasToken(body.accessToken)
    }

    @WithSpan("Texas.maskinporten")
    suspend fun maskinporten(@SpanAttribute("scopes") scopes: String): TexasToken {
        val requestBody = TokenRequest(identityProvider = "maskinporten", target = scopes)

        val response =
            texasHttpClient.post(config.tokenEndpoint) {
                contentType(ContentType.Application.Json)
                setBody(requestBody)
            }

        if (!response.status.isSuccess()) {
            response.logNonSuccess("maskinporten:$scopes")
            throw IllegalStateException("Unable to request m2m token for: maskinporten:$scopes")
        }

        val body = response.body<TokenResponse>()
        return TexasToken(body.accessToken)
    }

    private suspend fun HttpResponse.logNonSuccess(target: String) {
        if (this.contentType()?.isTextType() == true) {
            logger.error(
                "Unable to request m2m token for: ${target}, texas says: ${this.body<String>()}"
            )
        } else {
            logger.error(
                "Unable to request m2m token for: ${target}, texas responded with status ${this.status} and no content type"
            )
        }
    }

    internal data class TokenRequest(
        @param:JsonProperty("identity_provider") val identityProvider: String,
        val target: String,
    )

    internal data class TokenResponse(
        @param:JsonProperty("access_token") val accessToken: String,
        @param:JsonProperty("expires_in") val expiresIn: Int,
        @param:JsonProperty("token_type") val tokenType: String,
    )
}
