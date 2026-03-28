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
import io.github.jckoenen.sqs.utils.concurrentPartition
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

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
): Flow<Nel<MessageConsumer.Action>> =
    flatMapConcat { it.asFlow() }
        .concurrentPartition(
            concurrency = consumer.configuration.parallelism,
            partitionBy = { it.groupId },
            processingFn = consumer::handleSafely)
        .chunked(SQS_BATCH_SIZE, chunkWindow)

private fun Flow<Nel<Message.Fifo<String>>>.consumeFifoInBatch(
    consumer: MessageConsumer.Batch,
): Flow<Nel<MessageConsumer.Action>> =
    map { batch -> batch.groupNel { it.groupId } }
        .flatMapConcat { it.asIterable().asFlow() }
        .concurrentPartition(
            concurrency = consumer.configuration.parallelism,
            partitionBy = { (groupId, _) -> groupId },
            processingFn = { (_, messages) -> consumer.handleSafely(messages) })

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

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
private inline fun <A, B> Nel<A>.groupNel(groupFn: (A) -> B): Map<B, Nel<A>> =
    groupBy(groupFn).mapValues { (_, v) -> v.wrapAsNonEmptyListOrThrow() }
