package no.nav.tsm.ktor.nais

enum class RuntimeCluster(val nais: String) {
    LOCAL("local"),
    DEV("dev-gcp"),
    PROD("prod-gcp"),
}

fun getRuntimeCluster(): RuntimeCluster {
    val env = System.getenv("NAIS_CLUSTER_NAME") ?: "local"

    return when (env) {
        "dev-gcp" -> RuntimeCluster.DEV
        "prod-gcp" -> RuntimeCluster.PROD
        else -> RuntimeCluster.LOCAL
    }
}

class RuntimeInfo(
    val cluster: RuntimeCluster,
    val appName: String,
    val appNamespace: String,
)

fun getRuntimeInfo(): RuntimeInfo {
    val cluster = getRuntimeCluster()
    val appName = System.getenv("NAIS_APP_NAME") ?: "local-dev"
    val appNamespace = System.getenv("NAIS_NAMESPACE") ?: "local-dev"

    return RuntimeInfo(cluster, appName, appNamespace)
}
