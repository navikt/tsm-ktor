package no.nav.tsm.ktor.nais

import io.kotest.matchers.equals.shouldEqual
import io.kotest.matchers.ints.shouldBeAtLeast
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.server.application.install
import io.ktor.server.testing.testApplication
import kotlin.test.Test

class NaisMonitoringTest {
    @Test
    fun `should install metrics and health checks correctly`() = testApplication {
        application.install(NaisMonitoring)

        startApplication()

        client = createClient {}

        // Health checks
        client.get("/internal/health/alive").status.value shouldBe 200
        client.get("/internal/health/ready").status.value shouldBe 200

        // Prom Metrics
        val scrape = client.get("/internal/metrics").bodyAsText()
        scrape.split("\n").size shouldBeAtLeast 10
    }

    @Test
    fun `custom health checks should be good`() = testApplication {
        application.install(NaisMonitoring) {
            alive {
                check("database ready") {
                    // Health check fails
                    false
                }
            }

            ready {
                check("wehe") {
                    // Health check fails
                    false
                }
            }
        }

        startApplication()

        client = createClient {}

        val alive = client.get("/internal/health/alive")

        alive.status.value shouldBeGreaterThan 400
        alive.bodyAsText() shouldEqual """{"database ready":false}"""

        val ready = client.get("/internal/health/ready")
        ready.status.value shouldBeGreaterThan 400
        ready.bodyAsText() shouldEqual """{"wehe":false}"""
    }
}
