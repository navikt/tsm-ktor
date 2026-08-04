package no.nav.tsm.ktor.kafka.consumer

import com.fasterxml.jackson.databind.Module
import io.ktor.server.application.*
import java.util.Collections.emptyList
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

fun Application.createConsumer(
    groupId: String,
    topic: KafkaTopic<*>,
    pollDuration: Duration = 10.seconds,
    retryDuration: Duration = 60.seconds,
    jacksonModules: List<Module> = emptyList(),
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
    jacksonModules: List<Module> = emptyList(),
) =
    KafkaConsumerJob.initConsumerJob(
        application = this,
        handlers = topics,
        jobConfig =
            KafkaConsumerJobConfig(
                groupId = groupId,
                pollDuration = pollDuration,
                retryDuration = retryDuration,
                jacksonModules = jacksonModules.toMutableList(),
            ),
    )
