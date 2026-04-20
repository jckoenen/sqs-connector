package io.github.jckoenen.sqs.flow

import arrow.core.Either
import arrow.core.toNonEmptyListOrNull
import io.github.jckoenen.sqs.Failure
import io.github.jckoenen.sqs.Message
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
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.job

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

    val (channel, flow) =
        if (queue is Queue.Fifo) {
            val channel =
                receiveChannel(scope, visibilityManager) { receiveMessages(queue, receiveTimeout = 20.seconds) }
            channel to
                channel
                    .receiveAsFlow()
                    .buffer(consumer.configuration.parallelism)
                    .applyConsumerToFifoQueue(consumer, chunkWindow)
        } else {
            val channel =
                receiveChannel(scope, visibilityManager) { receiveMessages(queue, receiveTimeout = 20.seconds) }
            channel to
                channel
                    .receiveAsFlow()
                    .buffer(consumer.configuration.parallelism)
                    .applyConsumerToRegularQueue(consumer, chunkWindow)
        }

    flow
        .onEach { applyMessageActions(it, queue) }
        .maybe { visibilityManager?.trackOutbound(it) }
        .launchIn(scope)
        .invokeOnCompletion { ex ->
            when (ex) {
                null -> job.complete()
                is CancellationException -> job.cancel(ex)
                else -> job.completeExceptionally(ex)
            }
        }

    return ChannelDrainImpl(channel, job)
}

private inline fun <T : Message<*>> SqsConnector.receiveChannel(
    scope: CoroutineScope,
    manager: VisibilityManager?,
    crossinline fn: suspend SqsConnector.() -> Either<Failure, List<T>>
) =
    scope.produce {
        while (true) {
            val messages =
                retryIndefinitely(1.seconds, 1.minutes) { fn().warnOnLeft("Failed to poll messages. Retrying…") }
                    .toNonEmptyListOrNull()
            if (messages != null) {
                manager?.startTracking(messages, this)
                send(messages)
            } else {
                SqsConnector.logger.debug("Poll did not receive any messages")
            }
        }
    }

private data class ChannelDrainImpl(private val channel: ReceiveChannel<*>, override val job: Job) : DrainControl {
    override fun drain() = channel.cancel()
}

private inline fun <T> Flow<T>.maybe(f: (Flow<T>) -> Flow<T>?): Flow<T> = f(this) ?: this
