package io.github.jckoenen.sqs

import arrow.core.Either
import arrow.core.Ior
import arrow.core.Nel
import arrow.core.NonEmptyCollection
import aws.sdk.kotlin.services.sqs.SqsClient
import io.github.jckoenen.sqs.SqsFailure.ChangeMessagesFailure
import io.github.jckoenen.sqs.SqsFailure.DeleteMessagesFailure
import io.github.jckoenen.sqs.SqsFailure.GetQueueFailure
import io.github.jckoenen.sqs.SqsFailure.ReceiveMessagesFailure
import io.github.jckoenen.sqs.SqsFailure.SendMessagesFailure
import io.github.jckoenen.sqs.impl.kotlin.KotlinSqsConnector
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.slf4j.LoggerFactory

/**
 * The outcome of a batch operation, containing failed entries and their causes on the left, successfully sent messages
 * on the right.
 *
 * @param L the underlying failure type. Entries with the same failure will be grouped together
 * @param R the input/output type of the batch operation. Will be the same instances as passed in
 */
public typealias BatchResult<L, R> = Ior<FailuresWithCause<L, R>, Nel<R>>

/**
 * A map of failures to the entries that failed with that specific cause.
 *
 * @param L the failure type
 * @param R the entry type
 */
public typealias FailuresWithCause<L, R> = Map<out L, Nel<SqsConnector.FailedBatchEntry<R>>>

/**
 * A connector for interacting with AWS SQS. Provides high-level operations for queue management and message processing.
 */
public interface SqsConnector {

    /**
     * Information about an entry that failed during a batch operation.
     *
     * @param T the type of the reference object
     * @property reference the original object that failed to be processed
     * @property code the error code returned by SQS
     * @property errorMessage a human-readable description of the error
     * @property senderFault whether the error was caused by the sender
     */
    public data class FailedBatchEntry<T : Any>(
        val reference: T,
        val code: String,
        val errorMessage: String?,
        val senderFault: Boolean?,
    )

    public companion object {
        internal val logger = LoggerFactory.getLogger(SqsConnector::class.java)

        /**
         * Creates a new [SqsConnector] using the provided [SqsClient].
         *
         * @param client the AWS SQS client to use
         * @return a new [SqsConnector] instance
         */
        public operator fun invoke(client: SqsClient): SqsConnector = KotlinSqsConnector(client)
    }

    /**
     * Retrieves an existing queue by its name.
     *
     * @param name the name of the queue to retrieve
     * @return an [Either] containing the [Queue] or a [GetQueueFailure]
     */
    public suspend fun getQueue(name: Queue.Name): Either<GetQueueFailure, Queue>

    /**
     * Receives messages from the specified queue.
     *
     * @param queue the queue to receive messages from
     * @param receiveTimeout the maximum time to wait for messages (long polling)
     * @param visibilityTimeout the time during which received messages are invisible to other consumers
     * @return an [Either] containing the list of received [Message]s or a [ReceiveMessagesFailure]
     */
    public suspend fun receiveMessages(
        queue: Queue,
        receiveTimeout: Duration = 10.seconds,
        visibilityTimeout: Duration = 30.seconds
    ): Either<ReceiveMessagesFailure, List<Message<String>>>

    /**
     * Receives messages from the specified FIFO queue.
     *
     * @param queue the queue to receive messages from
     * @param receiveTimeout the maximum time to wait for messages (long polling)
     * @param visibilityTimeout the time during which received messages are invisible to other consumers
     * @return an [Either] containing the list of received [Message]s or a [ReceiveMessagesFailure]
     */
    public suspend fun receiveMessages(
        queue: Queue.Fifo,
        receiveTimeout: Duration = 10.seconds,
        visibilityTimeout: Duration = 30.seconds
    ): Either<ReceiveMessagesFailure, List<Message.Fifo<String>>>

    /**
     * Sends a batch of messages to the specified queue.
     *
     * @param queueUrl the URL of the target queue
     * @param messages a non-empty collection of messages to send
     * @return a [BatchResult] containing failures (if any) and successfully sent messages
     */
    public suspend fun sendMessages(
        queueUrl: Queue.Url,
        messages: NonEmptyCollection<OutboundMessage>,
    ): BatchResult<SendMessagesFailure, OutboundMessage>

    /**
     * Deletes a batch of messages from the specified queue.
     *
     * @param queueUrl the URL of the target queue
     * @param messages a non-empty collection of receipt handles for messages to delete
     * @return a [BatchResult] containing failures (if any) and successfully deleted receipt handles
     */
    public suspend fun deleteMessages(
        queueUrl: Queue.Url,
        messages: NonEmptyCollection<Message.ReceiptHandle>,
    ): BatchResult<DeleteMessagesFailure, Message.ReceiptHandle>

    /**
     * Extends the visibility timeout for a batch of messages.
     *
     * @param queueUrl the URL of the target queue
     * @param messages a non-empty collection of receipt handles for messages to update
     * @param duration the new visibility duration
     * @return a [BatchResult] containing failures (if any) and successfully updated receipt handles
     */
    public suspend fun extendMessageVisibility(
        queueUrl: Queue.Url,
        messages: NonEmptyCollection<Message.ReceiptHandle>,
        duration: Duration,
    ): BatchResult<ChangeMessagesFailure, Message.ReceiptHandle>
}
