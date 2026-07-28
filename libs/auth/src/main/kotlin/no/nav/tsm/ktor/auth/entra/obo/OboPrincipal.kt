package no.nav.tsm.ktor.auth.entra.obo

import io.ktor.server.auth.jwt.JWTCredential
import io.ktor.server.auth.principal
import io.ktor.server.routing.RoutingCall
import io.ktor.server.routing.path

data class EntraOnBehalfOfUser(
    /** EntraID claim: oid */
    val oid: String,
    /** EntraID claim: name */
    val name: String,
    /** EntraID claim: NAVident */
    val navIdent: String,
    /** EntraID claim: preferred_username */
    val email: String,
    /** EntraID claim: groups */
    val groups: List<String>,
)

fun RoutingCall.onBehalfOfUser(): EntraOnBehalfOfUser =
    requireNotNull(this.principal<EntraOnBehalfOfUser>()) {
        "No principal found on route ${this.route.path}, are you securing your routes with  entraOnBehalfOf { ... }?"
    }

internal fun JWTCredential.toEntraOnBehalfOfUser(): EntraOnBehalfOfUser {
    val oid = payload.getClaim("oid").asString()
    val name = payload.getClaim("name").asString()
    val navIdent = payload.getClaim("NAVident").asString()
    val email = payload.getClaim("preferred_username").asString()
    val groups = payload.getClaim("groups").asList(String::class.java)
    return EntraOnBehalfOfUser(oid, name, navIdent, email, groups)
}
