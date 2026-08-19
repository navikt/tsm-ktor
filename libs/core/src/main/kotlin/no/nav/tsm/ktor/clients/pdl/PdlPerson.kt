package no.nav.tsm.ktor.clients.pdl

import no.nav.tsm.ktor.core.Navn
import java.time.LocalDate

data class PdlPerson(val navn: PdlNavn?, val foedselsdato: LocalDate?, val identer: List<PdlIdent>)

data class PdlNavn(
    override val fornavn: String,
    override val mellomnavn: String?,
    override val etternavn: String,
) : Navn

data class PdlIdent(val ident: String, val gruppe: PdlIdentgruppe, val historisk: Boolean)

enum class PdlIdentgruppe {
    AKTORID,
    FOLKEREGISTERIDENT,
    NPID,
}
