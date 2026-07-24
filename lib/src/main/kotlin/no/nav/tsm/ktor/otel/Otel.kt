package no.nav.tsm.ktor.otel

import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode

fun failSpan(span: Span, event: String): String {
    span.setStatus(StatusCode.ERROR)
    span.addEvent(event)
    return event
}

fun String.failSpan(): String {
    val span = Span.current()

    return failSpan(span, this)
}

fun String.failSpan(span: Span): String {
    return failSpan(span, this)
}

fun failSpan(span: Span, exception: Throwable): Throwable {
    span.setStatus(StatusCode.ERROR)
    span.recordException(exception)
    return exception
}

fun Throwable.failSpan(): Throwable {
    val span = Span.current()

    return failSpan(span, this)
}

fun Throwable.failSpan(span: Span): Throwable {
    return failSpan(span, this)
}
