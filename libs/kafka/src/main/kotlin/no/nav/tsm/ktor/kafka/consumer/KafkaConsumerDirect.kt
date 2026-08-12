package no.nav.tsm.ktor.kafka.consumer

import io.ktor.server.application.*
import java.util.Collections.emptyList
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import tools.jackson.databind.JacksonModule

fun Application.createConsumer(
    groupId: String,
    topic: KafkaTopic<*>,
    pollDuration: Duration = 10.seconds,
    retryDuration: Duration = 60.seconds,
    jacksonModules: List<JacksonModule> = emptyList(),
) =
    createConsumer(
        groupId = groupId,
        topics = listOf(topic),
        pollDuration = pollDuration,
        retryDuration = retryDuration,
        jacksonModules = jacksonModules,
    )

fun Application.createConsumer(
    groupId: String,
    topics: List<KafkaTopic<*>>,
    pollDuration: Duration = 10.seconds,
    retryDuration: Duration = 60.seconds,
    closeTimeout: Duration = 5.seconds,
    shutdownTimeout: Duration = 5.seconds,
    jacksonModules: List<JacksonModule> = emptyList(),
) =
    KafkaConsumerJob.initConsumerJob(
        application = this,
        handlers = topics,
        jobConfig =
            KafkaConsumerJobConfig(
                groupId = groupId,
                pollDuration = pollDuration,
                retryDuration = retryDuration,
                closeTimeout = closeTimeout,
                shutdownTimeout = shutdownTimeout,
                jacksonModules = jacksonModules.toMutableList(),
            ),
    )
