package no.nav.tsm.ktor.kafka

import kotlin.test.Test
import kotlinx.coroutines.test.runTest

class StubTest {

    @Test
    fun `dummy`() = runTest {
        val stub: Stub = "Hello"
    }
}
