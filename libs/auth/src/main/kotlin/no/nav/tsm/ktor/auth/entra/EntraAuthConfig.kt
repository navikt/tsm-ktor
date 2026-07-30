package no.nav.tsm.ktor.auth.entra

internal const val ENTRA_ON_BEHALF_OF = "tsm-ktor-entra-obo-auth"

internal const val ENTRA_MACHINE_TOKEN = "tsm-ktor-entra-m2m-auth"

class EntraAuthConfig {
    private var _audience: String? = null
    var audience: String
        get() = _audience ?: getEnv(ExpectedEnvs.AZURE_APP_CLIENT_ID).also { _audience = it }
        set(value) {
            _audience = value
        }

    private var _issuer: String? = null
    var issuer: String
        get() = _issuer ?: getEnv(ExpectedEnvs.AZURE_OPENID_CONFIG_ISSUER).also { _issuer = it }
        set(value) {
            _issuer = value
        }

    private var _jwksUri: String? = null
    var jwksUri: String
        get() = _jwksUri ?: getEnv(ExpectedEnvs.AZURE_OPENID_CONFIG_JWKS_URI).also { _jwksUri = it }
        set(value) {
            _jwksUri = value
        }

    /**
     * Enables EntraID machine token validation and allows you to use entraMachineToken { ... } to protect your routes
     */
    var machine: Boolean = false

    /**
     * Enables EntraID on-behalf-of token validation and allows you to use entraOnBehalfOf { ... } to protect your
     * routes on behalf of a user, also lets you get the users principal from the Ktor route 'call'.
     */
    var obo: Boolean = false

    /**
     * Set this to true if you want to automatically stub the auth locally.
     *
     * This is based on the presence and value of NAIS_CLUSTER_NAME (injected by nais)
     */
    var autoStub: Boolean = false
}

private fun getEnv(env: ExpectedEnvs): String {
    return System.getenv(env.name) ?: throw MissingNaisEnvException(env.name)
}

private enum class ExpectedEnvs {
    AZURE_APP_CLIENT_ID,
    AZURE_OPENID_CONFIG_ISSUER,
    AZURE_OPENID_CONFIG_JWKS_URI,
}

internal class MissingNaisEnvException(env: String) :
    IllegalStateException("Missing $env environment variable, are you running in nais?")
