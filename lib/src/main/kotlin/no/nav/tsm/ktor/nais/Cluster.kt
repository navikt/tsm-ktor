package no.nav.tsm.ktor.nais

enum class RuntimeCluster(val nais: String) {
    LOCAL("local"),
    DEV("dev-gcp"),
    PROD("prod-gcp"),
}

internal fun getRuntimeCluster(): RuntimeCluster {
    val env = System.getenv("NAIS_CLUSTER_NAME") ?: "local"

    return when (env) {
        "dev-gcp" -> RuntimeCluster.DEV
        "prod-gcp" -> RuntimeCluster.PROD
        else -> RuntimeCluster.LOCAL
    }
}
