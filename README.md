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
    // Optional: If you need to consume from or produce to Kafka
    implementation(tsmKtorLibs.kafka)
    // Optional: Preconfigured consumer/producer for the sykmelding topics
    implementation(tsmKtorLibs.kafka.sykmeldinger)
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

**NAIS monitoring** — installs Prometheus metrics on `/internal/metrics`, liveness on `/internal/health/alive`, readiness on `/internal/health/ready`, shutdown on `/internal/shutdown`, plus call logging with a call-id from `Traceparent` or `X-Request-Id`.

```kotlin
install(NaisMonitoring)
```

Optionally add checks that must pass before the endpoints report OK:

```kotlin
install(NaisMonitoring) {
    alive { check("self") { true } }
    ready { check("database") { db.isReady() } }
}
```

Matching `nais.yaml`:

```yaml
liveness:
  path: /internal/health/alive
readiness:
  path: /internal/health/ready
preStopHook:
  http:
    path: /internal/shutdown
prometheus:
  enabled: true
  path: /internal/metrics
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

### kafka

Configuration is automatic: locally it connects to `BOOTSTRAP_SERVERS` (default `localhost:9092`) over PLAINTEXT, in the cloud it uses SSL with the `KAFKA_*` NAIS environment variables. Records are serialized as JSON with Jackson.

**Consumer** — subscribes to one or more topics, each with its own record type and handler. Offsets are committed manually per record, and only after the handler completes without throwing. On failure the consumer unsubscribes, waits `retryDuration` and reprocesses.

```kotlin
install(KafkaConsumer) {
    clientId = env.podName
    groupId = "tsm-my-app"

    consume<MyRecord>(
        name = "teamnavn.mitt-topic",
        onRecord = { record -> service.handle(record) },
        onTombstone = { meta -> service.delete(meta.key) },
    )
}
```

`onRecord` can also take the `RecordMeta` (key, topic, partition, offset, timestamp, headers) as a second parameter, and `shouldSkip` lets you commit and move past a poisoned record instead of retrying forever.

Use `batched` to receive every polled record for a topic in one call. The whole batch is committed only if the handler returns without throwing.

```kotlin
batched<MyRecord>("teamnavn.mitt-topic") { records ->
    // records: List<Pair<Sykmelding?, RecordMeta>>, null value means tombstone
    service.handleAll(records)
}
```

**Producer** — `createProducer` returns a producer for a single topic. Requires the `KafkaProducer` plugin (or `KafkaConfig`) to be installed.

```kotlin
install(KafkaProducer) { clientId = env.podName }

val producer = createProducer<MyRecord>("teamnavn.mitt-topic")
producer.send(key = record.id, value = record)
producer.tombstone(key = record.id)
```

**Standalone consumer** — `createConsumer` gives you a `KafkaConsumerJob` you can `start()` and `stop()` yourself, instead of tying it to the Ktor life-cycle.

```kotlin
install(KafkaConfig) { clientId = env.podName }

val job = createConsumer(
    groupId = "tsm-my-app",
    topic = onRecord<MyRecord>(
        name = "teamnavn.mitt-topic",
        onRecord = { service.handle(it) },
        onTombstone = { service.delete(it.key) },
    ),
)
```

Custom Jackson modules can be registered with `jacksonModule(...)` in the consumer plugin, or the `jacksonModules` parameter on `createProducer`/`createConsumer`.

### kafka-sykmeldinger

Preconfigured consumer and producer for the sykmelding topics, with the right topic names, headers and Jackson modules already in place.

**Consumer** — reads `SykmeldingRecord` from `tsm.sykmeldinger`.

```kotlin
install(SykmeldingerConsumer) {
    clientId = env.podName
    groupId = "tsm-my-app"
    onRecord = { record -> service.handle(record) }
    onTombstone = { meta -> service.delete(meta.key) }
}
```

**Producer** — writes `SykmeldingRecord` to `tsm.sykmeldinger-input`, keyed on the sykmelding id and with the required `source-app` and `source-namespace` headers set from the NAIS environment.

```kotlin
install(KafkaProducer) { clientId = env.podName }

val producer = sykmeldingInputProducer()
producer.send(record)
producer.tombstone(sykmeldingId)
```
