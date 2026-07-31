package no.nav.tsm.ktor.kafka

import io.kotest.matchers.equals.shouldEqual
import kotlin.test.Test
import kotlinx.coroutines.test.runTest

class Stub {
    @Test
    fun `stub`() = runTest {
        true shouldEqual true
    }
}
