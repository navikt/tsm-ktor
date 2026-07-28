package no.nav.tsm.ktor.auth.texas

import no.nav.tsm.ktor.nais.RuntimeCluster

class TexasConfiguration(
    val tokenEndpoint: String =
        System.getenv("NAIS_TOKEN_ENDPOINT")
            ?: throw IllegalStateException("Missing TEXAS_TOKEN_ENDPOINT environment variable"),
    val introspectionEndpoint: String =
        System.getenv("NAIS_TOKEN_INTROSPECTION_ENDPOINT")
            ?: throw IllegalStateException(
                "Missing TEXAS_INTROSPECTION_ENDPOINT environment variable"
            ),
)

enum class TexasTarget(val nais: String) {
    DEV("dev-gcp"),
    PROD("prod-gcp"),
    DEV_FSS("dev-fss"),
    PROD_FSS("prod-fss"),
}

fun RuntimeCluster.toTexasTarget(): TexasTarget =
    when (this) {
        RuntimeCluster.DEV -> TexasTarget.DEV
        RuntimeCluster.PROD -> TexasTarget.PROD
        else -> throw IllegalStateException("Unsupported cluster for Texas: $this")
    }
