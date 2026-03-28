package io.github.jckoenen.sqs

import io.github.jckoenen.sqs.Message.Fifo.DeduplicationId
import io.github.jckoenen.sqs.Message.GroupId

/**
 * A message received from an SQS queue.
 *
 * @param T the type of the message content
 */
public interface Message<out T : Any> : MessageBound {
    /** Unique identifier for a message. */
    @JvmInline public value class Id(public val value: String)

    /** An identifier used to delete the message or change its visibility. */
    @JvmInline public value class ReceiptHandle(public val value: String)

    /** The tag that specifies that a message belongs to a specific message group. */
    @JvmInline public value class GroupId(public val value: String)

    /** The unique identifier of the message. */
    public val id: Id
    /** The message attributes. */
    public val attributes: Map<String, String>
    /** The deserialized content of the message. */
    public val content: T

    /** The group identifier of the message. */
    public val groupId: GroupId?

    /** Features specific to messages from FIFO queues. */
    public interface Fifo<T : Any> : Message<T> {
        /** The token used for deduplication of sent messages. */
        @JvmInline public value class DeduplicationId(public val value: String)

        /** The group identifier of the message. */
        override val groupId: GroupId
        /** The deduplication identifier of the message. */
        public val deduplicationId: DeduplicationId
    }
}

internal data class MessageImpl<T : Any>(
    override val id: Message.Id,
    override val receiptHandle: Message.ReceiptHandle,
    override val attributes: Map<String, String>,
    override val content: T,
    override val queue: Queue,
    override val groupId: GroupId?
) : Message<T>

internal data class FifoMessageImpl<T : Any>(
    override val id: Message.Id,
    override val receiptHandle: Message.ReceiptHandle,
    override val attributes: Map<String, String>,
    override val content: T,
    override val queue: Queue.Fifo,
    override val groupId: GroupId,
    override val deduplicationId: DeduplicationId,
) : Message.Fifo<T>
