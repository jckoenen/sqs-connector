package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.raise.either
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.Message as SqsMessage
import aws.sdk.kotlin.services.sqs.model.MessageSystemAttributeName
import aws.sdk.kotlin.services.sqs.receiveMessage
import io.github.jckoenen.sqs.FifoMessageImpl
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.MessageImpl
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsFailure.ReceiveMessagesFailure
import io.github.jckoenen.sqs.utils.id
import kotlin.time.Duration

internal const val RECEIVE_OPERATION = "SQS.ReceiveMessages"

internal suspend fun SqsClient.receiveMessages(
    queue: Queue,
    receiveTimeout: Duration,
    visibilityTimeout: Duration,
) = either {
    val response =
        execute<ReceiveMessagesFailure, _>(convertCommonExceptions(queue.id(), RECEIVE_OPERATION)) {
                receiveMessage {
                    maxNumberOfMessages = SQS_BATCH_SIZE
                    waitTimeSeconds = receiveTimeout.inWholeSeconds.toInt()
                    queueUrl = queue.url.value
                    messageAttributeNames = listOf("*")
                    messageSystemAttributeNames = MessageSystemAttributeName.values()
                    this.visibilityTimeout = visibilityTimeout.inWholeSeconds.toInt()
                }
            }
            .bind()

    response.messages.orEmpty().map { message ->
        val attributes = message.mergedAttributes()
        val id = Message.Id(message.messageId!!)
        val receiptHandle = Message.ReceiptHandle(message.receiptHandle!!)
        val groupId = attributes[MessageSystemAttributeName.MessageGroupId]?.let(Message<*>::GroupId)
        val dedupeId =
            attributes[MessageSystemAttributeName.MessageDeduplicationId]?.let(Message.Fifo<*>::DeduplicationId)

        if (queue is Queue.Fifo) {
            FifoMessageImpl(id, receiptHandle, attributes, message.body.orEmpty(), queue, groupId!!, dedupeId!!)
        } else {
            MessageImpl(id, receiptHandle, attributes, message.body.orEmpty(), queue, groupId)
        }
    }
}

private operator fun Map<String, String>.get(attributeName: MessageSystemAttributeName) =
    get(attributeName.value)?.takeUnless(String::isBlank)

private fun SqsMessage.mergedAttributes(): Map<String, String> {
    val custom =
        messageAttributes
            .orEmpty()
            .filter { (_, v) -> v.dataType != "Binary" }
            .mapNotNull { (k, v) -> v.stringValue?.takeUnless(String::isBlank)?.let { k to it } }
    val system = attributes.orEmpty().mapNotNull { (k, v) -> v.takeUnless(String::isBlank)?.let { k.value to it } }

    return (custom + system).toMap()
}
