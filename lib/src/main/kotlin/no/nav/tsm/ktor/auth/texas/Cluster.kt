package no.nav.tsm.ktor.auth.texas

enum class TargetCluster(val nais: String) {
    LOCAL("local"),
    DEV("dev-gcp"),
    PROD("prod-gcp"),
}

internal fun getRuntimeCluster(): TargetCluster {
    val env = System.getenv("NAIS_CLUSTER_NAME") ?: "local"

    return when(env) {
        "dev-gcp" -> TargetCluster.DEV
        "prod-gcp" -> TargetCluster.PROD
        else -> TargetCluster.LOCAL
    }
}