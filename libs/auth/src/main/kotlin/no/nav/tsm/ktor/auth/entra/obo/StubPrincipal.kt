package no.nav.tsm.ktor.auth.entra.obo

val stubPrincipal =
    EntraOnBehalfOfUser(
        oid = "00000000-0000-0000-0000-000000000000",
        name = "Stub User",
        navIdent = "Z123456",
        email = "stub.user@example.com",
        groups = listOf(),
    )
