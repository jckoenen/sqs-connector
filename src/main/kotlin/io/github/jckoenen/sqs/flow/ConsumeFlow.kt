package io.github.jckoenen.sqs.flow

import io.github.jckoenen.sqs.MessageConsumer
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.utils.asTags
import io.github.jckoenen.sqs.utils.id
import io.github.jckoenen.sqs.utils.mdc
import io.github.jckoenen.sqs.utils.resolveQueue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onEach

// These are just best guess
private const val CHUNK_WINDOW_FACTOR = .6
private const val VISIBILITY_OFFSET_FACTOR = 0.2

/**
 * Consumes messages from the specified queue using the provided consumer. This function returns a [DrainableFlow]
 * which, when collected, will start the consumption process.
 *
 * @param queue the queue to consume from
 * @param consumer the consumer to use for processing messages
 * @param enableAutomaticVisibilityExtension whether to automatically extend message visibility while processing
 * @param visibilityTimeout the initial visibility timeout for received messages
 * @return a [DrainableFlow] that can be started and gracefully stopped
 */
public fun SqsConnector.consume(
    queue: Queue,
    consumer: MessageConsumer,
    enableAutomaticVisibilityExtension: Boolean = true,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nothing> {
    check(visibilityTimeout.isFinite() && visibilityTimeout.isPositive()) {
        "visibilityTimeout must be finite and positive, got $visibilityTimeout"
    }
    return drainableImpl {
        val visibilityManager =
            if (enableAutomaticVisibilityExtension) {
                VisibilityManager(this@consume, visibilityTimeout, visibilityTimeout * VISIBILITY_OFFSET_FACTOR)
            } else {
                null
            }
        val chunkWindow = visibilityTimeout * CHUNK_WINDOW_FACTOR

        val receiver =
            if (queue is Queue.Fifo) {
                receive(queue, visibilityTimeout)
                    .maybe { visibilityManager?.trackInbound(it) }
                    .applyConsumerToFifoQueue(consumer, chunkWindow)
            } else {
                receive(queue, visibilityTimeout)
                    .maybe { visibilityManager?.trackInbound(it) }
                    .applyConsumerToRegularQueue(consumer, chunkWindow)
            }

        receiver
            .onEach { applyMessageActions(it, queue) }
            .maybe { visibilityManager?.trackOutbound(it) }
            .flowOn(mdc(queue.id().asTags()))
            .collect {}
    }
}

/**
 * Consumes messages from the specified queue (by name) using the provided consumer. This function returns a
 * [DrainableFlow] which, when collected, will start the consumption process.
 *
 * @param queueName the name of the queue to consume from
 * @param consumer the consumer to use for processing messages
 * @param enableAutomaticVisibilityExtension whether to automatically extend message visibility while processing
 * @param visibilityTimeout the initial visibility timeout for received messages
 * @return a [DrainableFlow] that can be started and gracefully stopped
 */
public fun SqsConnector.consume(
    queueName: Queue.Name,
    consumer: MessageConsumer,
    enableAutomaticVisibilityExtension: Boolean = true,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nothing> = drainableImpl {
    val q = resolveQueue(queueName)

    consume(q, consumer, enableAutomaticVisibilityExtension, visibilityTimeout).collect(::emit)
}

private inline fun <T> Flow<T>.maybe(f: (Flow<T>) -> Flow<T>?): Flow<T> = f(this) ?: this
