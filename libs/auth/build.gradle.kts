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
    api(project(":libs:core"))
    api(platform(ktorLibs.bom))
    implementation(ktorLibs.server.auth)
    implementation(ktorLibs.server.auth.jwt)

    testImplementation(ktorLibs.server.testHost)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.kotest.assertions)
    testImplementation(libs.junit.jupiter.engine)
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
        kotlin { ktfmt("0.62").kotlinlangStyle() }
        check {
            dependsOn("spotlessApply")
        }
    }

}

publishing {
    publications {
        create<MavenPublication>("gpr") {
            from(components["java"])
            groupId = "no.nav.tsm"
            artifactId = "ktor-auth"
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
