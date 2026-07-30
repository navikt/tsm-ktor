package no.nav.tsm.ktor.kafka

import io.ktor.server.testing.testApplication
import kotlin.test.Test
import org.testcontainers.kafka.ConfluentKafkaContainer

class KafkaTest {
    companion object {
        val kafka =
            ConfluentKafkaContainer("confluentinc/cp-kafka:8.1.0").apply {
                start()
            }
    }

    @Test
    fun `dummy`() = testApplication {
        val stub: Stub = "Hello"
    }
}
