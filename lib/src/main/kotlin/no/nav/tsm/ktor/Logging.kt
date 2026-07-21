package no.nav.tsm.ktor

import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal fun logger(): Logger =
    LoggerFactory.getLogger(
        StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).callerClass
    )
