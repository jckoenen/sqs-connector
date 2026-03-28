package io.github.jckoenen.sqs.utils

import io.github.jckoenen.sqs.impl.kotlin.flattenToList
import io.github.jckoenen.sqs.testinfra.ProjectKotestConfiguration.Companion.eventually
import io.kotest.core.spec.style.FreeSpec
import io.kotest.core.test.testCoroutineScheduler
import io.kotest.matchers.collections.shouldBeMonotonicallyIncreasingWith
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlin.time.Duration.Companion.INFINITE
import kotlin.time.Duration.Companion.milliseconds

class TimedChunkTest : FreeSpec({
    val itemCount = 100
    val itemDelay = 10.milliseconds
    val chunkInterval = 25.milliseconds
    val chunkSize = 4

    fun countingFlow() = generateSequence(0, Int::inc).asFlow()

    "should preserve emission order".config(coroutineTestScope = true) - {
        "on natural emissions" {
            val actual =
                countingFlow().take(itemCount).chunked(chunkSize, INFINITE).flattenToList()

            actual shouldBeMonotonicallyIncreasingWith naturalOrder()
            actual shouldHaveSize itemCount
        }
        "on timed emissions" {
            val actual =
                countingFlow()
                    .take(itemCount)
                    .onEach { delay(itemDelay) }
                    .chunked(chunkSize, chunkInterval)
                    .flattenToList()

            actual shouldBeMonotonicallyIncreasingWith naturalOrder()
            actual shouldHaveSize itemCount
        }
    }

    "should respect backpressure".config(coroutineTestScope = true) - {
        "on natural emissions" {
            val suspendAt = 4
            val gate = FlowGate(suspendAt)
            val seen = mutableListOf<Int>()
            var lastProduced = 0
            val job =
                countingFlow()
                    .take(itemCount)
                    .onEach { lastProduced = it }
                    .chunked(chunkSize, INFINITE)
                    // flatMapConcat uses a large buffer, but we want to test suspension
                    .buffer(capacity = Channel.RENDEZVOUS)
                    .flatMapConcat(List<Int>::asFlow)
                    .onEach(seen::add)
                    .let(gate::applyTo)
                    .launchIn(this)

            gate.awaitPaused()
            seen.last() shouldBe suspendAt
            job.isCompleted shouldBe false

            eventually {
                val before = lastProduced
                testCoroutineScheduler.advanceUntilIdle()
                val after = lastProduced
                after shouldBe before
            }

            gate.paused = false
            job.join()
            seen shouldBeMonotonicallyIncreasingWith naturalOrder()
            seen shouldHaveSize itemCount
        }
        "on timed emissions" {
            val suspendAt = 4
            val gate = FlowGate(suspendAt)
            val seen = mutableListOf<Int>()
            var lastProduced = 0
            val job =
                countingFlow()
                    .take(itemCount)
                    .onEach { lastProduced = it }
                    .onEach { delay(itemDelay) }
                    .chunked(chunkSize, chunkInterval)
                    // flatMapConcat uses a large buffer, but we want to test suspension
                    .buffer(capacity = Channel.RENDEZVOUS)
                    .flatMapConcat(List<Int>::asFlow)
                    .onEach(seen::add)
                    .let(gate::applyTo)
                    .launchIn(this)

            gate.awaitPaused()
            seen.last() shouldBe suspendAt
            job.isCompleted shouldBe false

            eventually {
                val before = lastProduced
                testCoroutineScheduler.advanceUntilIdle()
                val after = lastProduced
                after shouldBe before
            }

            gate.paused = false
            job.join()
            seen shouldBeMonotonicallyIncreasingWith naturalOrder()
            seen shouldHaveSize itemCount
        }
    }
})

private class FlowGate(val suspendAt: Int) {
    private val signal = MutableStateFlow(false)

    var paused by signal::value

    suspend fun awaitPaused() = signal.first { it }

    suspend fun awaitActive() = signal.first { !it }

    fun applyTo(flow: Flow<Int>) =
        flow.onEach { value ->
            if (value == suspendAt) {
                paused = true
                awaitActive()
            }
        }
}
