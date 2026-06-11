package io.github.jckoenen.sqs.flow

import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.wrapAsNonEmptyListOrThrow
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.MessageConsumer.Action
import io.github.jckoenen.sqs.OutboundMessage
import io.github.jckoenen.sqs.testinfra.ProjectKotestConfiguration.Companion.eventually
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension.queueName
import io.github.jckoenen.sqs.testinfra.TestMessageConsumer
import io.github.jckoenen.sqs.testinfra.assumeRight
import io.kotest.assertions.nondeterministic.eventuallyConfig
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldNotContainAnyOf
import io.kotest.matchers.ints.beGreaterThanOrEqualTo
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.job
import kotlin.time.Duration.Companion.seconds

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
class ConsumeLifecycleTest : FreeSpec({
    "SqsConnector.consumeIn lifecycle" - {
        val connector = SqsContainerExtension.newConnector()
        val visibilityTimeout = 3.seconds

        fun contents(count: Int) =
            generateSequence(0, Int::inc).take(count).map { it.toString() }.toList().wrapAsNonEmptyListOrThrow()

        "a successfully processed message is delivered to the consumer exactly once" {
            val queue = connector.getOrCreateQueue(queueName(), createDlq = true).assumeRight()
            val expected = contents(25)
            connector.sendMessages(queue.url, expected.map(::OutboundMessage)).assumeRight()

            val consumer = TestMessageConsumer.create { Action.DeleteMessage(it) }
            connector.consumeIn(queue, consumer, this, visibilityTimeout = visibilityTimeout)

            eventually {
                consumer.seen.value.map(Message<String>::content) shouldContainExactlyInAnyOrder expected
            }

            // wait well past the visibility timeout: a deleted message must never be redelivered
            delay(visibilityTimeout * 2)
            consumer.seen.value.map(Message<String>::content) shouldContainExactlyInAnyOrder expected

            currentCoroutineContext().job.cancelChildren()
        }

        "draining stops polling, finishes the in-flight message and completes the job" {
            val queue = connector.getOrCreateQueue(queueName(), createDlq = true).assumeRight()
            connector.sendMessages(queue.url, contents(1).map(::OutboundMessage)).assumeRight()

            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val consumer = TestMessageConsumer.create { msg ->
                started.complete(Unit)
                release.await()
                Action.DeleteMessage(msg)
            }

            val control = connector.consumeIn(queue, consumer, this, visibilityTimeout = visibilityTimeout)

            started.await() // the message is now in-flight inside the consumer
            control.drain()

            // the in-flight message is still being processed, so the job must not have completed yet
            control.job.isCompleted shouldBe false

            release.complete(Unit)
            control.job.join()

            // the job (poll loop, consumer and visibility manager) completed normally, not via cancellation
            control.job.isCompleted shouldBe true
            control.job.isCancelled shouldBe false
            consumer.seen.value.map(Message<String>::content) shouldContainExactlyInAnyOrder contents(1)

            currentCoroutineContext().job.cancelChildren()
        }

        "draining processes already-polled messages and leaves the rest on the queue" {
            val queue = connector.getOrCreateQueue(queueName(), createDlq = true).assumeRight()
            val parallelism = 5
            val expected = contents(200) // more than fits the internal buffers, so messages are left behind
            connector.sendMessages(queue.url, expected.map(::OutboundMessage)).assumeRight()

            val inFlight = MutableStateFlow(0)
            val release = CompletableDeferred<Unit>()
            val drained = TestMessageConsumer.create(parallelism = parallelism) { msg ->
                inFlight.update(Int::inc)
                release.await()
                Action.DeleteMessage(msg)
            }

            val control = connector.consumeIn(queue, drained, this, visibilityTimeout = visibilityTimeout)

            inFlight.first { it >= parallelism } // saturate the consumer so messages are certainly in-flight
            control.drain()
            release.complete(Unit)
            control.job.join()

            control.job.isCancelled shouldBe false

            // everything that was polled is processed, at least the messages that were in-flight
            val drainedContents = drained.seen.value.map(Message<String>::content)
            drainedContents.size should beGreaterThanOrEqualTo(parallelism)

            // a fresh consumer picks up exactly the messages the drained one never polled — nothing is lost or dropped.
            // a batch that was caught mid-send when polling stopped is released back to the queue and becomes visible
            // again after its (configured) receive-visibility expires.
            val rest = TestMessageConsumer.create { Action.DeleteMessage(it) }
            connector.consumeIn(queue, rest, this, visibilityTimeout = visibilityTimeout)

            eventually(eventuallyConfig { duration = 20.seconds }) {
                (drainedContents + rest.seen.value.map(Message<String>::content)) shouldContainExactlyInAnyOrder expected
            }
            drainedContents shouldNotContainAnyOf rest.seen.value.map(Message<String>::content)

            currentCoroutineContext().job.cancelChildren()
        }

        "cancelling stops polling, cancels the consumer and the visibility manager" {
            val queue = connector.getOrCreateQueue(queueName(), createDlq = true).assumeRight()
            connector.sendMessages(queue.url, contents(1).map(::OutboundMessage)).assumeRight()

            val started = CompletableDeferred<Unit>()
            val handlerCancelled = CompletableDeferred<Unit>()
            val consumer = TestMessageConsumer.create { _ ->
                started.complete(Unit)
                try {
                    awaitCancellation()
                } finally {
                    handlerCancelled.complete(Unit)
                }
            }

            val control = connector.consumeIn(queue, consumer, this, visibilityTimeout = visibilityTimeout)

            started.await()
            control.job.cancel()
            control.job.join()

            // join only returns once the job AND all of its children completed. The poll loop and every visibility
            // task are children of this job, so a completed-via-cancellation job means polling and visibility
            // management have stopped too.
            control.job.isCancelled shouldBe true
            handlerCancelled.isCompleted shouldBe true // the in-flight consumer was actually cancelled

            // end-to-end proof that visibility management stopped: the message was never deleted, so with nothing
            // extending its visibility any more it must become available again once the receive-visibility expires.
            val rest = TestMessageConsumer.create { Action.DeleteMessage(it) }
            connector.consumeIn(queue, rest, this, visibilityTimeout = visibilityTimeout)

            eventually(eventuallyConfig { duration = 15.seconds }) {
                rest.seen.value.map(Message<String>::content) shouldContainExactlyInAnyOrder contents(1)
            }

            currentCoroutineContext().job.cancelChildren()
        }
    }
})
