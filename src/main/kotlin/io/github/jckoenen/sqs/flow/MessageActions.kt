package io.github.jckoenen.sqs.flow

import arrow.core.Nel
import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.flatten
import arrow.core.leftIor
import arrow.core.toNonEmptyListOrThrow
import arrow.core.wrapAsNonEmptyListOrThrow
import io.github.jckoenen.sqs.BatchResult
import io.github.jckoenen.sqs.Failure
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.MessageConsumer.Action
import io.github.jckoenen.sqs.MessageConsumer.Action.DeleteMessage
import io.github.jckoenen.sqs.MessageConsumer.Action.MoveMessageToDlq
import io.github.jckoenen.sqs.MessageConsumer.Action.RetryBackoff
import io.github.jckoenen.sqs.OutboundMessage
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.SqsFailure
import io.github.jckoenen.sqs.allTags
import io.github.jckoenen.sqs.impl.kotlin.SEND_OPERATION
import io.github.jckoenen.sqs.impl.kotlin.batchCallFailed
import io.github.jckoenen.sqs.impl.kotlin.reduce
import io.github.jckoenen.sqs.utils.TypedMap.Companion.byType
import io.github.jckoenen.sqs.utils.id
import io.github.jckoenen.sqs.utils.putAll
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map

internal suspend fun SqsConnector.applyMessageActions(actions: List<Action>, queue: Queue) {
    val byType = actions.byType()

    byType.onMatching { moveToDlq(it, queue).logOutcome(it) }
    byType.onMatching { delete(it, queue).logOutcome(it) }
    byType.onMatching { backoff(it, queue).logOutcome(it) }
}

private suspend fun SqsConnector.moveToDlq(
    toSend: Nel<MoveMessageToDlq>,
    queue: Queue,
): BatchResult<SqsFailure, *> {
    val lookup = toSend.map(MoveMessageToDlq::message).associateBy(OutboundMessage.Companion::fromMessage)
    val outbound = lookup.keys.toNonEmptyListOrThrow()
    val dlq = queue.dlq

    return if (dlq == null) {
        val failure =
            SqsFailure.QueueDoesNotExist(
                operation = SEND_OPERATION,
                queue = queue.id(),
                message = "The specified queue does not have a DLQ! Messages will be retried instead",
            )
        batchCallFailed(failure, outbound, senderFault = true).leftIor()
    } else {
        val sendResult = sendMessages(dlq.url, outbound)

        sendResult
            .mapLeft { it.mapKeys { (key, _) -> key as SqsFailure } } // up-cast to simplify merging below
            .map { sent -> sent.map(lookup::getValue).map(Message<*>::receiptHandle) }
            .map { toDelete -> deleteMessages(queue.url, toDelete) }
            .flatten { l, r -> l + r }
    }
}

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
private suspend fun SqsConnector.backoff(toSend: Nel<RetryBackoff>, queue: Queue) =
    toSend
        .groupBy(RetryBackoff::backoffDuration, RetryBackoff::receiptHandle)
        .mapValues { (_, handles) -> handles.wrapAsNonEmptyListOrThrow() } // groupBy guarantees being non-empty
        .entries
        .asFlow()
        .map { (duration, handles) -> extendMessageVisibility(queue.url, handles, duration) }
        .reduce()

private suspend fun SqsConnector.delete(toDelete: Nel<DeleteMessage>, queue: Queue) =
    deleteMessages(queue.url, toDelete.map(DeleteMessage::receiptHandle))

private inline fun <reified A : Action> BatchResult<Failure, *>.logOutcome(ignored: Nel<A>) {
    getOrNull()?.let { success ->
        SqsConnector.logger
            .atDebug()
            .addKeyValue("sqs.action", A::class.simpleName)
            .addKeyValue("sqs.messages.count", success.size)
            .log("Action succeeded")
    }

    leftOrNull()?.forEach { (cause, failures) ->
        val errorsByCode = failures.groupingBy { it.code }.eachCount()
        SqsConnector.logger
            .atWarn()
            .addKeyValue("sqs.action", A::class.simpleName)
            .addKeyValue("sqs.messages.count", failures.size)
            .addKeyValue("sqs.failure.codes", errorsByCode)
            .putAll(cause.allTags())
            .log("Action (partially?) failed")
    }
}
