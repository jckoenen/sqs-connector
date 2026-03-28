package io.github.jckoenen.sqs.flow

import arrow.core.Either
import arrow.core.Nel
import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.getOrElse
import arrow.core.wrapAsNonEmptyListOrThrow
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.MessageConsumer
import io.github.jckoenen.sqs.MessageConsumer.Action.RetryBackoff
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.impl.kotlin.SQS_BATCH_SIZE
import io.github.jckoenen.sqs.utils.chunked
import kotlin.math.absoluteValue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach

private val EXCEPTION_BACKOFF = 1.minutes

internal fun Flow<Nel<Message<String>>>.applyConsumerToRegularQueue(consumer: MessageConsumer, chunkWindow: Duration) =
    when (consumer) {
        is MessageConsumer.Individual ->
            flatMapMerge(consumer.configuration.parallelism, List<Message<String>>::asFlow)
                .map(consumer::handleSafely)
                .chunked(SQS_BATCH_SIZE, chunkWindow)

        is MessageConsumer.Batch ->
            flatMapMerge(consumer.configuration.parallelism) { batch -> flow { emit(consumer.handleSafely(batch)) } }
    }

internal fun Flow<Nel<Message.Fifo<String>>>.applyConsumerToFifoQueue(
    consumer: MessageConsumer,
    chunkWindow: Duration
): Flow<Nel<MessageConsumer.Action>> =
    when (consumer) {
        is MessageConsumer.Individual -> consumeFifoIndividually(consumer, chunkWindow)
        is MessageConsumer.Batch -> consumeFifoInBatch(consumer)
    }

private fun Flow<Nel<Message.Fifo<String>>>.consumeFifoIndividually(
    consumer: MessageConsumer.Individual,
    chunkWindow: Duration
): Flow<Nel<MessageConsumer.Action>> {
    val groupedProcessing = channelFlow {
        val partitions = List(consumer.configuration.parallelism) { Channel<Message.Fifo<String>>() }

        partitions.forEach { partition ->
            partition.consumeAsFlow().map(consumer::handleSafely).onEach { send(it) }.launchIn(this)
        }

        this@consumeFifoIndividually.flatMapConcat { it.asFlow() }
            .collect { message ->
                val partition = message.groupId.value.hashCode().absoluteValue % consumer.configuration.parallelism
                partitions[partition].send(message)
            }
        partitions.forEach { it.close() }
    }
    return groupedProcessing.chunked(SQS_BATCH_SIZE, chunkWindow)
}

private fun Flow<Nel<Message.Fifo<String>>>.consumeFifoInBatch(
    consumer: MessageConsumer.Batch,
): Flow<Nel<MessageConsumer.Action>> = channelFlow {
    val partitions = List(consumer.configuration.parallelism) { Channel<Nel<Message.Fifo<String>>>() }

    partitions.forEach { partition ->
        partition.consumeAsFlow().map(consumer::handleSafely).onEach { send(it) }.launchIn(this)
    }

    this@consumeFifoInBatch.map { batch -> batch.groupBy { it.groupId } }
        .flatMapConcat { it.asIterable().asFlow() }
        .collect { (id, messages) ->
            val partition = id.value.hashCode().absoluteValue % consumer.configuration.parallelism
            @OptIn(PotentiallyUnsafeNonEmptyOperation::class)
            partitions[partition].send(messages.wrapAsNonEmptyListOrThrow())
        }

    partitions.forEach { it.close() }
}

private suspend fun MessageConsumer.Individual.handleSafely(message: Message<String>) =
    Either.catch { handle(message) }
        .onLeft {
            SqsConnector.logger
                .atError()
                .addKeyValue("sqs.consumer", this::class)
                .setCause(it)
                .addKeyValue("sqs.message.id", message.id)
                .log(
                    "Consumer threw uncaught exception, message will be retried after $EXCEPTION_BACKOFF. " +
                        "To suppress this message, return MessageConsumer.Action.RetryBackoff instead.")
        }
        .getOrElse { RetryBackoff(message, EXCEPTION_BACKOFF) }

private suspend fun MessageConsumer.Batch.handleSafely(messages: Nel<Message<String>>) =
    Either.catch { handle(messages) }
        .onLeft {
            SqsConnector.logger
                .atError()
                .addKeyValue("sqs.consumer", this::class)
                .setCause(it)
                .addKeyValue("sqs.message.ids", messages.map(Message<*>::id))
                .log(
                    "Consumer threw uncaught exception, messages will be retried after $EXCEPTION_BACKOFF. " +
                        "To suppress this message, return MessageConsumer.Action.RetryBackoff instead.")
        }
        .getOrElse { messages.map { RetryBackoff(it, EXCEPTION_BACKOFF) } }
