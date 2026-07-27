package no.nav.tsm.ktor

import org.slf4j.Logger
import org.slf4j.LoggerFactory

fun logger(): Logger =
    LoggerFactory.getLogger(StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).callerClass)

/**
 * This requires configuration in nais.yaml to work, as well as configuration in logback.xml!
 */
fun teamLogger(): Logger =
    LoggerFactory.getLogger("teamlog.${StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).callerClass}")
