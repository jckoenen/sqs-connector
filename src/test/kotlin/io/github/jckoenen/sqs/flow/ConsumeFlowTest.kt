package io.github.jckoenen.sqs.flow

import arrow.core.Nel
import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.wrapAsNonEmptyListOrThrow
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.MessageConsumer
import io.github.jckoenen.sqs.MessageConsumer.Action
import io.github.jckoenen.sqs.OutboundMessage
import io.github.jckoenen.sqs.testinfra.ProjectKotestConfiguration.Companion.eventually
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension.fifoQueueName
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension.queueName
import io.github.jckoenen.sqs.testinfra.TestMessageConsumer
import io.github.jckoenen.sqs.testinfra.assumeRight
import io.kotest.assertions.fail
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.string.shouldContain
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.job
import org.slf4j.MDC
import kotlin.time.Duration.Companion.seconds

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
class ConsumeFlowTest: FreeSpec ({
    "Using SqsConnector.consume" - {
        val connector = SqsContainerExtension.newConnector()
        val visibilityTimeout = 3.seconds

        "messages moved to dlq should not be received again" {
            val queue = connector.getOrCreateQueue(queueName(), createDlq = true)
                .assumeRight()
            val dlq = queue.dlq.shouldNotBeNull()

            val expected =
                generateSequence(0, Int::inc)
                    .take(25)
                    .map { it.toString() }
                    .toList()
                    .wrapAsNonEmptyListOrThrow()

            connector.sendMessages(queue.url, expected.map { OutboundMessage(it) })
                .assumeRight()

            val dlqConsumer = TestMessageConsumer.create(handleFn = Action::DeleteMessage)
            val qConsumer = TestMessageConsumer.create(handleFn = Action::MoveMessageToDlq)
            qConsumer.seen.launchIn(this)
            dlqConsumer.seen.launchIn(this)

            connector.consume(queue, qConsumer, visibilityTimeout = visibilityTimeout)
                .launchWithDrainControl(this)

            connector.consume(dlq, dlqConsumer, visibilityTimeout = visibilityTimeout)
                .launchWithDrainControl(this)

            eventually {
                val inDlq = dlqConsumer.seen.value.map(Message<String>::content)
                inDlq shouldContainExactlyInAnyOrder expected
            }

            delay(visibilityTimeout * 2)
            val seenInQueue = qConsumer.seen.value.map(Message<String>::content)
            seenInQueue shouldContainExactlyInAnyOrder expected

            currentCoroutineContext().job.cancelChildren()
        }

        "should have MDC available in consumers" {
            val queue = connector.getOrCreateQueue(queueName(), createDlq = true)
                .assumeRight()

                generateSequence(0, Int::inc)
                    .take(2)
                    .map { it.toString() }
                    .map(::OutboundMessage)
                    .toList()
                    .wrapAsNonEmptyListOrThrow()
                    .let { connector.sendMessages(queue.url, it)}
                    .assumeRight()

            val mdc = CompletableDeferred<Map<String, String>?>()

            val consumer = TestMessageConsumer.create {
                mdc.complete(MDC.getCopyOfContextMap())
                Action.DeleteMessage(it)
            }
            connector.consume(queue, consumer).launchWithDrainControl(this)

            with(mdc.await()) {
                shouldNotBeNull()

                keys shouldContain "sqs.queue.url"
                keys shouldContain "sqs.queue.name"
            }

            currentCoroutineContext().job.cancelChildren()
        }
    }
}
)
