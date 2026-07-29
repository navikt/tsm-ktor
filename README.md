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

## Features

### core

**Logging** — loggers named after the calling class. `teamLogger()` writes to team logs (secure logs, for sensitive data).

```kotlin
private val logger = logger()
private val teamLog = teamLogger()
```

**Runtime cluster** — reads `NAIS_CLUSTER_NAME`, defaults to `LOCAL`.

```kotlin
if (getRuntimeCluster() == RuntimeCluster.PROD) { ... }
```

**Dynamic dependencies** — register different implementations locally and in the cloud.

```kotlin
dynamicDependencies {
    local { provide(PdlLocalClient::class) }
    cloud { provide(PdlCloudClient::class) }
}
```

**Texas client** — fetches Entra ID and Maskinporten tokens from the NAIS token endpoint.

```kotlin
val (token) = texasClient.entraIdToken("tsm", "tsm-pdl-cache")
val (token) = texasClient.maskinporten("nhn:hpr/basic")
```

**OpenTelemetry** — mark the current span as failed, returns its receiver.

```kotlin
logger.error("Body parsing failed", e.failSpan())
```

### auth

**Entra authentication** — validates Entra ID tokens. Audience, issuer and JWKS URI default to the `AZURE_*` NAIS environment variables. `autoStub` bypasses validation when running locally.

```kotlin
install(EntraAuth) {
    machine = true
    obo = true
    autoStub = true
}
```

**Protected routes** — `entraMachineToken` for machine-to-machine, `entraOnBehalfOf` for logged-in users.

```kotlin
routing {
    entraMachineToken {
        get("/api/sykmelding") { ... }
    }
    entraOnBehalfOf {
        get("/internal/admin/jobs") {
            val user = call.onBehalfOfUser()
            call.respondText(user.navIdent)
        }
    }
}
```
