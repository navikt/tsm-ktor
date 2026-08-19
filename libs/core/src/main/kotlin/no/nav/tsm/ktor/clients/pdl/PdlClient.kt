package no.nav.tsm.ktor.clients.pdl

sealed interface PdlClient {
    class UnknownError(cause: Throwable) : Exception(cause)

    suspend fun getPerson(ident: String): PdlPerson?

    suspend fun getAktorId(ident: String): String?
}
