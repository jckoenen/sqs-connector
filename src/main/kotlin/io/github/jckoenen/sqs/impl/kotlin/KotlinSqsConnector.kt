package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.NonEmptyCollection
import aws.sdk.kotlin.services.sqs.SqsClient
import io.github.jckoenen.sqs.BatchResult
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.OutboundMessage
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.SqsFailure
import io.github.jckoenen.sqs.SqsFailure.ChangeMessagesFailure
import io.github.jckoenen.sqs.SqsFailure.CreateQueueFailure
import io.github.jckoenen.sqs.SqsFailure.DeleteMessagesFailure
import kotlin.time.Duration

internal class KotlinSqsConnector(
    private val sqsClient: SqsClient,
) : SqsConnector {

    override suspend fun getQueue(name: Queue.Name): Either<SqsFailure.GetQueueFailure, Queue> =
        sqsClient.getQueue(name)

    // TODO: move to test sources or make public
    /**
     * Retrieves an existing queue or creates it if it doesn't exist.
     *
     * @param name the name of the queue
     * @param createDlq whether to also create a Dead Letter Queue for this queue
     * @return an [Either] containing the [Queue] or a [CreateQueueFailure]
     */
    internal suspend fun getOrCreateQueue(
        name: Queue.Name,
        createDlq: Boolean,
    ): Either<CreateQueueFailure, Queue> = sqsClient.getOrCreateQueue(name, createDlq)

    override suspend fun receiveMessages(
        queue: Queue,
        receiveTimeout: Duration,
        visibilityTimeout: Duration,
    ): Either<SqsFailure.ReceiveMessagesFailure, List<Message<String>>> =
        sqsClient.receiveMessages(queue, receiveTimeout, visibilityTimeout)

    @Suppress("UNCHECKED_CAST") // the implementation produces correct messages by queue type
    override suspend fun receiveMessages(
        queue: Queue.Fifo,
        receiveTimeout: Duration,
        visibilityTimeout: Duration
    ): Either<SqsFailure.ReceiveMessagesFailure, List<Message.Fifo<String>>> =
        sqsClient.receiveMessages(queue, receiveTimeout, visibilityTimeout)
            as Either<SqsFailure.ReceiveMessagesFailure, List<Message.Fifo<String>>>

    override suspend fun sendMessages(
        queueUrl: Queue.Url,
        messages: NonEmptyCollection<OutboundMessage>,
    ): BatchResult<SqsFailure.SendMessagesFailure, OutboundMessage> = sqsClient.sendMessages(queueUrl, messages)

    override suspend fun deleteMessages(
        queueUrl: Queue.Url,
        messages: NonEmptyCollection<Message.ReceiptHandle>,
    ): BatchResult<DeleteMessagesFailure, Message.ReceiptHandle> = sqsClient.deleteMessages(queueUrl, messages)

    override suspend fun extendMessageVisibility(
        queueUrl: Queue.Url,
        messages: NonEmptyCollection<Message.ReceiptHandle>,
        duration: Duration,
    ): BatchResult<ChangeMessagesFailure, Message.ReceiptHandle> =
        sqsClient.extendMessageVisibility(queueUrl, messages, duration)
}
