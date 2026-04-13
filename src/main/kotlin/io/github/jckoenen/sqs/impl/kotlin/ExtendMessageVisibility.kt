package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.Nel
import arrow.core.NonEmptyCollection
import arrow.core.leftIor
import arrow.core.toNonEmptyListOrNull
import arrow.core.unzip
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.changeMessageVisibilityBatch
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry
import io.github.jckoenen.sqs.BatchResult
import io.github.jckoenen.sqs.FailuresWithCause
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.SqsFailure
import io.github.jckoenen.sqs.SqsFailure.ChangeMessagesFailure
import kotlin.time.Duration
import kotlinx.coroutines.flow.map

internal const val CHANGE_OPERATION = "SQS.ChangeMessageVisibilities"

internal suspend fun SqsClient.extendMessageVisibility(
    queueUrl: Queue.Url,
    messages: NonEmptyCollection<Message.ReceiptHandle>,
    duration: Duration,
): BatchResult<ChangeMessagesFailure, Message.ReceiptHandle> =
    messages
        .chunkForBatching { i, handle ->
            ChangeMessageVisibilityBatchRequestEntry {
                id = i.toString()
                receiptHandle = handle.value
                visibilityTimeout = duration.inWholeSeconds.toInt()
            }
        }
        .map { chunk ->
            val (inChunk, batch) = chunk.unzip()

            doChange(queueUrl, batch, inChunk)
        }
        .reduce()

private suspend fun SqsClient.doChange(
    queueUrl: Queue.Url,
    batch: Nel<ChangeMessageVisibilityBatchRequestEntry>,
    inChunk: Nel<Message.ReceiptHandle>,
): BatchResult<ChangeMessagesFailure, Message.ReceiptHandle> =
    execute<ChangeMessagesFailure, _>(convertCommonExceptions(queueUrl.leftIor(), CHANGE_OPERATION)) {
            changeMessageVisibilityBatch {
                this.queueUrl = queueUrl.value
                entries = batch
            }
        }
        .mapLeft { batchCallFailed(it, inChunk) }
        .fold(
            ifLeft = { it.leftIor() },
            ifRight = {
                splitFailureAndSuccess(CHANGE_OPERATION, queueUrl.leftIor(), inChunk, it.failed)
                    .mapLeft(::extractAlreadyDeleted)
            },
        )

private fun <T : Any> extractAlreadyDeleted(
    map: FailuresWithCause<SqsFailure.PartialFailure, T>
): FailuresWithCause<ChangeMessagesFailure, T> {
    return map.entries
        .flatMap { (cause, affected) ->
            val (alreadyDeleted, others) = affected.partition(::isAlreadyDeleted)

            val toBeKept = others.toNonEmptyListOrNull()?.let { cause to it }
            val newCause =
                alreadyDeleted.toNonEmptyListOrNull()?.let {
                    ChangeMessagesFailure.MessageAlreadyDeleted(cause.queue) to it
                }
            listOf(toBeKept, newCause)
        }
        .filterNotNull()
        .groupBy({ (k, _) -> k }, { (_, v) -> v })
        .mapValues { (_, v) -> v.reduce { l, r -> l + r } }
}

private fun isAlreadyDeleted(entry: SqsConnector.FailedBatchEntry<*>) =
    entry.senderFault == false &&
        (entry.code == "InvalidParameterValueException" ||
            entry.code == "ReceiptHandleIsInvalid" ||
            entry.code == "MessageNotInflight")
