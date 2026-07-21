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
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation(libs.junit.jupiter.engine)

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // This dependency is exported to consumers, that is to say found on their compile classpath.
    api(libs.commons.math3)

    // This dependency is used internally, and not exposed to consumers on their own compile classpath.
    implementation(libs.guava)
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
