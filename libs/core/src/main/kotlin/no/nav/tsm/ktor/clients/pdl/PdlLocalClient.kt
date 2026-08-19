package no.nav.tsm.ktor.clients.pdl

import no.nav.tsm.ktor.logger
import java.time.LocalDate

class PdlLocalClient : PdlClient {
    private val logger = logger()

    override suspend fun getPerson(ident: String): PdlPerson? {
        if (ident == "does-not-exist") {
            logger.info("[PDL Mock]: Got request for ident that does not exist, returning null")
            return null
        }

        logger.info("[PDL Mock]: Got request for ident $ident, returning mock person")
        return PdlPerson(
            navn = PdlNavn(fornavn = "Test", mellomnavn = "Testesen", etternavn = "Testson"),
            foedselsdato = LocalDate.parse("1990-01-01"),
            identer =
                listOf(
                    PdlIdent(
                        ident = ident,
                        gruppe = PdlIdentgruppe.FOLKEREGISTERIDENT,
                        historisk = false,
                    )
                ),
        )
    }
}
