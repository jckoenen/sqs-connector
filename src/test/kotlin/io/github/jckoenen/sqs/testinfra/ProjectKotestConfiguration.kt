package io.github.jckoenen.sqs.testinfra

import io.kotest.assertions.nondeterministic.EventuallyConfiguration
import io.kotest.assertions.nondeterministic.eventuallyConfig
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class ProjectKotestConfiguration : AbstractProjectConfig() {
    override val extensions: List<Extension> = listOf(SqsContainerExtension)

    override val failOnEmptyTestSuite: Boolean = true
    override val coroutineDebugProbes: Boolean = true

    companion object {
        // https://github.com/kotest/kotest/issues/5147
        suspend fun <T> eventually(
            config: EventuallyConfiguration = eventuallyConfig { duration = 10.seconds },
            f: suspend () -> T,
        ) = withContext(Dispatchers.Default) { io.kotest.assertions.nondeterministic.eventually(config, f) }
    }
}
