plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`

    id("maven-publish")
}

version = file("version").readText().trim()

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.ktor.client.test.mock)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.kotest.assertions)
    testImplementation(libs.junit.jupiter.engine)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    api(platform(libs.ktor.bom))
    api(libs.ktor.client.core)
    api(libs.ktor.server.di)
    api(libs.otel.annotations)

    implementation(libs.ktor.client.content.negotiation)
    implementation(libs.ktor.serialization.jackson)
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
