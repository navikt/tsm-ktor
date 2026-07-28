# tsm-ktor

A set of utilities and other useful classes for working with Ktor in Team Symfoni.

## Usage

Add the version catalog in `settings.gradle.kts`:

```kotlin
dependencyResolutionManagement {
    repositories {
        maven("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
    }
    versionCatalogs {
        create("tsmKtorLibs").from("no.nav.tsm:ktor-version-catalog:<version>")
    }
}
```

Then add dependencies in `build.gradle.kts`:

```kotlin
dependencies {
    implementation(tsmKtorLibs.core)
    // Optional: If you need to secure your API with authentication
    implementation(tsmKtorLibs.auth)
}
```
