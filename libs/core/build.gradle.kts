import com.diffplug.gradle.spotless.SpotlessExtension
import org.gradle.kotlin.dsl.check
import org.gradle.kotlin.dsl.invoke

plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`

    id("maven-publish")
    alias(libs.plugins.spotless)
}

version = file("../version").readText().trim()

dependencies {
    api(platform(ktorLibs.bom))
    api(ktorLibs.client.core)
    api(ktorLibs.server.di)
    api(ktorLibs.server.metrics.micrometer)
    api(ktorLibs.server.callId)
    api(libs.otel.annotations)
    api(libs.khealth)
    api(libs.slf4j)
    api(libs.micrometer.registry.prometheus)

    implementation(ktorLibs.client.contentNegotiation)
    implementation(ktorLibs.serialization.jackson3)

    testImplementation(ktorLibs.server.testHost)
    testImplementation(ktorLibs.client.mock)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.kotest.assertions)
    testImplementation(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.logback.classic)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
        withSourcesJar()
    }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks {
    configure<SpotlessExtension> {
        kotlin {
            ktfmt("0.64").kotlinlangStyle().configure {
                it.setMaxWidth(120)
                it.setContinuationIndent(4)
            }
        }
        check {
            dependsOn("spotlessApply")
        }
    }

}

val generateVersionFile =
    tasks.register("generateVersionFile") {
        val version = project.version.toString()
        val outputDir = layout.buildDirectory.dir("generated/tsm-ktor")
        outputs.dir(outputDir)

        doLast {
            val file = outputDir.get().file("Version.kt").asFile
            file.parentFile.mkdirs()
            file.writeText(
                """
            package no.nav.tsm.ktor

            internal object LibraryVersion {
                const val VERSION = "$version"
            }
            
            """
                    .trimIndent()
            )
        }
    }

sourceSets["main"].kotlin.srcDir(generateVersionFile.map { it.outputs.files })

publishing {
    publications {
        create<MavenPublication>("gpr") {
            from(components["java"])
            groupId = "no.nav.tsm"
            artifactId = "ktor"
            version = version
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/navikt/tsm-ktor")
            credentials {
                username = "x-access-token"
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}
