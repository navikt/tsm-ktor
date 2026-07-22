package no.nav.tsm.ktor.auth.texas

class TexasConfiguration(
    val tokenEndpoint: String = System.getenv("NAIS_TOKEN_ENDPOINT")
        ?: throw IllegalStateException("Missing TEXAS_TOKEN_ENDPOINT environment variable"),
    val introspectionEndpoint: String = System.getenv("NAIS_TOKEN_INTROSPECTION_ENDPOINT")
        ?: throw IllegalStateException("Missing TEXAS_INTROSPECTION_ENDPOINT environment variable")
)
