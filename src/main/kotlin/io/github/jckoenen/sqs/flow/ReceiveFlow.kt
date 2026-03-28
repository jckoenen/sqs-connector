package io.github.jckoenen.sqs.flow

import arrow.core.Either
import arrow.core.Nel
import arrow.core.toNonEmptyListOrNull
import io.github.jckoenen.sqs.Failure
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.allTags
import io.github.jckoenen.sqs.utils.putAll
import io.github.jckoenen.sqs.utils.resolveQueue
import io.github.jckoenen.sqs.utils.retryIndefinitely
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onEach

/**
 * Continuously receives messages from the specified SQS queue as a stream. The method uses indefinite retry logic to
 * handle transient polling errors and processes incoming messages as a drainable flow.
 *
 * @param queue the SQS queue from which messages are to be received
 * @param visibilityTimeout the duration for which a polled message is invisible to other clients
 * @return a [DrainableFlow] emitting non-empty lists of messages
 */
public fun SqsConnector.receive(
    queue: Queue,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nel<Message<String>>> = receiveImpl { receiveMessages(queue, visibilityTimeout = visibilityTimeout) }

/**
 * Continuously receives messages from the specified SQS FIFO queue as a stream. The method uses indefinite retry logic
 * to handle transient polling errors and processes incoming messages as a drainable flow.
 *
 * @param queue the SQS queue from which messages are to be received
 * @param visibilityTimeout the duration for which a polled message is invisible to other clients
 * @return a [DrainableFlow] emitting non-empty lists of messages
 */
public fun SqsConnector.receive(
    queue: Queue.Fifo,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nel<Message.Fifo<String>>> = receiveImpl {
    receiveMessages(queue, visibilityTimeout = visibilityTimeout)
}

private fun <T : Message<*>> SqsConnector.receiveImpl(
    fn: suspend SqsConnector.() -> Either<Failure, List<T>>
): DrainableFlow<Nel<T>> {
    val messagesFlow = flow {
        while (true) {
            val messages =
                retryIndefinitely(1.seconds, 1.minutes) { fn().warnOnLeft("Failed to poll messages. Retrying…") }
            emit(messages)
        }
    }
    return messagesFlow
        .onEach { if (it.isEmpty()) SqsConnector.logger.debug("No messages received") }
        .mapNotNull { it.toNonEmptyListOrNull() }
        .drainable()
}

/**
 * Receives messages from an SQS queue (by name) as a drainable flow. This method retries indefinitely to resolve the
 * queue name and then continuously polls for messages.
 *
 * @param queueName the name of the SQS queue from which messages are to be received
 * @param visibilityTimeout the duration a retrieved message should remain invisible to other consumers
 * @return a [DrainableFlow] emitting non-empty lists of messages
 */
public fun SqsConnector.receive(
    queueName: Queue.Name,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nel<Message<String>>> =
    channelFlow {
            val queue = resolveQueue(queueName)
            receive(queue, visibilityTimeout).collect(::send)
        }
        .drainable()

private fun <F : Failure, T> Either<F, T>.warnOnLeft(message: String) = onLeft {
    SqsConnector.logger.atWarn().putAll(it.allTags()).log(message)
}
