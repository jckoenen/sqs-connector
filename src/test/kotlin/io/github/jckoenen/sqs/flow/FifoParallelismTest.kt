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
import io.github.jckoenen.sqs.testinfra.TestMessageConsumer
import io.github.jckoenen.sqs.testinfra.assumeRight
import io.kotest.core.spec.style.FreeSpec
import io.kotest.inspectors.forAllKeys
import io.kotest.matchers.collections.shouldBeSortedBy
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.nulls.shouldNotBeNull
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.job
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.seconds

class FifoParallelismTest : FreeSpec({
    "FIFO queue with parallelism" - {
        val connector = SqsContainerExtension.newConnector()
        val visibilityTimeout = 3.seconds

        "should process all messages from multiple groups" {
            val queue = connector.getOrCreateQueue(fifoQueueName(), createDlq = false)
                .assumeRight()

            val outbound = createMessages(listOf("a", "b", "c"))
            connector.sendMessages(queue.url, outbound).assumeRight()

            val consumer = TestMessageConsumer.create(parallelism = 3) { Action.DeleteMessage(it) }

            connector.consumeIn(queue, consumer, this, visibilityTimeout = visibilityTimeout)

            eventually {
                val received = consumer.seen.value.map(Message<String>::content)
                received shouldContainExactlyInAnyOrder outbound.map { it.content }
            }

            currentCoroutineContext().job.cancelChildren()
        }

        "should preserve ordering within each message group" {
            val queue = connector.getOrCreateQueue(fifoQueueName(), createDlq = false)
                .assumeRight()

            val groups = setOf("order-a", "order-b", "order-c")
            val outbound = createMessages(groups, 10)
            connector.sendMessages(queue.url, outbound).assumeRight()

            val consumer = TestMessageConsumer.create(parallelism = 3) { Action.DeleteMessage(it) }

            connector.consumeIn(queue, consumer, this, visibilityTimeout = visibilityTimeout)

            val seen = consumer.seen
                .firstOrNull { it.size == outbound.size }

            // Within each group, messages must arrive in order
            val grouped = seen.orEmpty().groupBy { it.groupId?.value }
            grouped.keys shouldContainExactly groups
            grouped.forAllKeys { k ->
                grouped[k].shouldNotBeNull() shouldBeSortedBy Message<String>::content
            }

            currentCoroutineContext().job.cancelChildren()
        }

        "should work with batch consumer" {
            val queue = connector.getOrCreateQueue(fifoQueueName(), createDlq = false)
                .assumeRight()

            val outbound = createMessages(listOf("batch-a", "batch-b"), 5)
            connector.sendMessages(queue.url, outbound).assumeRight()

            val seen = ConcurrentHashMap.newKeySet<String>()
            val consumer = object : MessageConsumer.Batch {
                override val configuration = object : MessageConsumer.Configuration {
                    override val parallelism: Int get() = 2
                }

                override suspend fun handle(messages: Nel<Message<String>>): Nel<Action> {
                    messages shouldBeSortedBy Message<String>::content
                    seen += messages.map { it.content }
                    return messages.map(Action::DeleteMessage)
                }
            }

            connector.consumeIn(queue, consumer, this, visibilityTimeout = visibilityTimeout)

            eventually {
                seen shouldContainExactlyInAnyOrder outbound.map { it.content }
            }

            currentCoroutineContext().job.cancelChildren()
        }
    }
})

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
private fun createMessages(groups: Collection<String>, perGroup: Int = 5) =
    groups.flatMap { group ->
        List(perGroup) { i ->
            OutboundMessage(
                content = "$group:$i",
                groupId = Message.GroupId(group),
                deduplicationId = Message.Fifo.DeduplicationId("$group:$i"),
            )
        }
    }.wrapAsNonEmptyListOrThrow()
