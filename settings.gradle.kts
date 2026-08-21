rootProject.name = "tsm-ktor"

include(":libs:core")
include(":libs:auth")
include(":libs:kafka")
include(":libs:kafka-test")
include(":libs:kafka-sykmeldinger")
include(":libs:catalog")

val ktor = "3.5.2"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven("https://jitpack.io")
        maven("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
    }
    versionCatalogs {
        create("ktorLibs").from("io.ktor:ktor-version-catalog:$ktor")
    }
}

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
        maven("https://jitpack.io")
    }
}

plugins {
    id("io.github.ben-manes.versions.settings") version "0.61.0"
}
