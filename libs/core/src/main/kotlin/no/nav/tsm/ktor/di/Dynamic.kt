package no.nav.tsm.ktor.di

import io.ktor.server.application.*
import io.ktor.server.plugins.di.*
import no.nav.tsm.ktor.nais.RuntimeCluster
import no.nav.tsm.ktor.nais.getRuntimeCluster

fun Application.dynamicDependencies(block: DynamicDependenciesScope.() -> DynamicDependenciesScope) {
    DynamicDependenciesScope(isLocal(), this).block()
}

class DynamicDependenciesScope(private val isLocal: Boolean, private val application: Application) {
    fun local(block: DependencyRegistry.() -> Unit): DynamicDependenciesScope {
        if (isLocal) application.dependencies(block)
        return this
    }

    fun cloud(block: DependencyRegistry.() -> Unit): DynamicDependenciesScope {
        if (!isLocal) application.dependencies(block)
        return this
    }
}

internal fun isLocal() = getRuntimeCluster() === RuntimeCluster.LOCAL
