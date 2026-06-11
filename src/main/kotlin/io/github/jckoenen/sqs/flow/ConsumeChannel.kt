package io.github.jckoenen.sqs.flow

import arrow.core.Either
import arrow.core.Nel
import arrow.core.toNonEmptyListOrNull
import io.github.jckoenen.sqs.Failure
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.MessageBound
import io.github.jckoenen.sqs.MessageConsumer
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.utils.asTags
import io.github.jckoenen.sqs.utils.id
import io.github.jckoenen.sqs.utils.mdc
import io.github.jckoenen.sqs.utils.retryIndefinitely
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

// These are just best guess
private const val CHUNK_WINDOW_FACTOR = .6
private const val VISIBILITY_OFFSET_FACTOR = 0.2

/**
 * Consume messages from the specified queue using the provided consumer
 *
 * @param queue the queue to consume from
 * @param consumer the consumer to use for processing messages
 * @param enableAutomaticVisibilityExtension whether to automatically extend message visibility while processing
 * @param visibilityTimeout the initial visibility timeout for received messages
 * @return a [DrainControl] that allows to gracefully stop consumption
 */
public fun SqsConnector.consumeIn(
    queue: Queue,
    consumer: MessageConsumer,
    parentScope: CoroutineScope,
    enableAutomaticVisibilityExtension: Boolean = true,
    visibilityTimeout: Duration = 30.seconds,
): DrainControl = consumeImpl(queue, consumer, parentScope, enableAutomaticVisibilityExtension, visibilityTimeout)

private fun SqsConnector.consumeImpl(
    queue: Queue,
    consumer: MessageConsumer,
    parentScope: CoroutineScope,
    enableAutomaticVisibilityExtension: Boolean,
    visibilityTimeout: Duration,
): DrainControl {
    check(visibilityTimeout.isFinite() && visibilityTimeout.isPositive()) {
        "visibilityTimeout must be finite and positive, got $visibilityTimeout"
    }

    val job = Job(parentScope.coroutineContext.job)
    val ctx = parentScope.coroutineContext + job + CoroutineName("consume-${queue.name}") + mdc(queue.id().asTags())
    val scope = CoroutineScope(ctx)

    val visibilityManager =
        if (enableAutomaticVisibilityExtension) {
            VisibilityManager(this@consumeImpl, visibilityTimeout, visibilityTimeout * VISIBILITY_OFFSET_FACTOR)
        } else {
            null
        }

    val chunkWindow = visibilityTimeout * CHUNK_WINDOW_FACTOR

    val (pollJob, flow) =
        if (queue is Queue.Fifo) {
            val (channel, pollJob) =
                receiveChannel(scope, visibilityManager) {
                    receiveMessages(queue, receiveTimeout = 20.seconds, visibilityTimeout = visibilityTimeout)
                }
            val flow =
                channel
                    .receiveAsFlow()
                    .buffer(consumer.configuration.parallelism)
                    .applyConsumerToFifoQueue(consumer, chunkWindow)

            pollJob to flow
        } else {
            val (channel, pollJob) =
                receiveChannel(scope, visibilityManager) {
                    receiveMessages(queue, receiveTimeout = 20.seconds, visibilityTimeout = visibilityTimeout)
                }
            val flow =
                channel
                    .receiveAsFlow()
                    .buffer(consumer.configuration.parallelism)
                    .applyConsumerToRegularQueue(consumer, chunkWindow)

            pollJob to flow
        }

    flow
        .onEach { applyMessageActions(it, queue) }
        .stopTracking(visibilityManager)
        .launchIn(scope)
        .invokeOnCompletion { ex ->
            when (ex) {
                null -> job.complete()
                is CancellationException -> job.cancel(ex)
                else -> job.completeExceptionally(ex)
            }
        }

    return ConsumeDrainImpl(pollJob, job)
}

/**
 * Starts a poll-loop coroutine that feeds received message batches into a newly created [Channel].
 *
 * The returned [Job] is the poll-loop itself. Cancelling it (the way [ConsumeDrainImpl.drain] does it) interrupts any
 * in-flight poll and closes the channel _normally_ via the `finally` block, so downstream flow collectors see a clean
 * end-of-stream and can finish processing buffered messages.
 *
 * Visibility-extension tasks are launched against the outer [scope] (not the poll-loop), so they outlive drain and keep
 * extending visibility for messages already in the downstream pipeline until those messages reach
 * [VisibilityManager.stopTracking].
 *
 * If a polled batch is tracked but fails to be delivered to the channel (e.g. because the poll-loop is cancelled during
 * [Channel.send]), the catch block untracks it under [NonCancellable] so the matching visibility task can finish.
 */
private inline fun <T : Message<*>> SqsConnector.receiveChannel(
    scope: CoroutineScope,
    manager: VisibilityManager?,
    crossinline pollFn: suspend SqsConnector.() -> Either<Failure, List<T>>
): Pair<ReceiveChannel<Nel<T>>, Job> {
    val channel = Channel<Nel<T>>(Channel.RENDEZVOUS)
    val pollJob =
        scope.launch(CoroutineName("sqs-poll")) {
            try {
                while (true) {
                    pollOnce(pollFn, manager, scope, channel)
                }
            } finally {
                channel.close()
            }
        }
    return channel to pollJob
}

private suspend inline fun <T : Message<*>> SqsConnector.pollOnce(
    pollFn: suspend SqsConnector.() -> Either<Failure, List<T>>,
    manager: VisibilityManager?,
    scope: CoroutineScope,
    channel: Channel<Nel<T>>
) {
    val messages =
        retryIndefinitely(1.seconds, 1.minutes) { pollFn().warnOnLeft("Failed to poll messages. Retrying…") }
            .toNonEmptyListOrNull()

    if (messages == null) {
        SqsConnector.logger.debug("Poll did not receive any messages")
        return
    }

    try {
        // track messages before sending it downstream, so that they don't expire if sending suspends
        manager?.startTracking(messages, scope)
        channel.send(messages)
    } catch (e: Throwable) {
        // if there's any failure in tracking or sending, we must let go of the message so that it becomes available
        // again
        withContext(NonCancellable) { messages.forEach { manager?.stopTracking(it) } }
        throw e
    }
}

private data class ConsumeDrainImpl(private val pollJob: Job, override val job: Job) : DrainControl {
    override fun drain() {
        pollJob.cancel(CancellationException("Drain requested"))
    }
}

private fun <T : MessageBound, C : Collection<T>> Flow<C>.stopTracking(manager: VisibilityManager?) =
    if (manager == null) this else onEach { batch -> batch.forEach { manager.stopTracking(it) } }
