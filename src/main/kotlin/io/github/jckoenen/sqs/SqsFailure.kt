package io.github.jckoenen.sqs

import aws.smithy.kotlin.runtime.SdkBaseException
import io.github.jckoenen.sqs.utils.QueueId
import io.github.jckoenen.sqs.utils.asTags
import io.github.jckoenen.sqs.utils.opTag

/** Represents failures that can occur when interacting with SQS. */
public sealed interface SqsFailure : Failure {
    public sealed interface GetQueueFailure : SqsFailure

    public sealed interface CreateQueueFailure : SqsFailure, GetQueueFailure

    public sealed interface ReceiveMessagesFailure : SqsFailure

    public sealed interface SendMessagesFailure : SqsFailure

    public sealed interface DeleteMessagesFailure : SqsFailure

    public sealed interface ChangeMessagesFailure : SqsFailure

    public data class QueueDoesNotExist(
        val operation: String,
        val queue: QueueId,
        override val message: String = "The target queue does not exist"
    ) : ReceiveMessagesFailure, SendMessagesFailure, DeleteMessagesFailure, ChangeMessagesFailure, GetQueueFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + queue.asTags()
    }

    public data class UnknownFailure(
        val operation: String,
        val queue: QueueId,
        val cause: Exception,
        val additionalTags: Map<String, Any> = emptyMap(),
    ) :
        GetQueueFailure,
        CreateQueueFailure,
        ReceiveMessagesFailure,
        SendMessagesFailure,
        DeleteMessagesFailure,
        ChangeMessagesFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + additionalTags + queue.asTags()

        override val message: String
            get() = cause.message ?: "Exception did not provide a message"
    }

    public data class Throttled(val operation: String, val queue: QueueId) :
        GetQueueFailure,
        CreateQueueFailure,
        ReceiveMessagesFailure,
        SendMessagesFailure,
        DeleteMessagesFailure,
        ChangeMessagesFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + queue.asTags()

        override val message: String
            get() = "Call failed due to AWS throttling"
    }

    public data class KmsFailure(val operation: String, val cause: SdkBaseException, val queue: QueueId) :
        ReceiveMessagesFailure, SendMessagesFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + queue.asTags()

        override val message: String
            get() = "Couldn't read messages due to underlying KMS issue"
    }

    public data class PartialFailure(val operation: String, val queue: QueueId) :
        SendMessagesFailure, DeleteMessagesFailure, ChangeMessagesFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + queue.asTags()

        override val message: String
            get() = "Some entries in this batch request could not be processed"
    }
}
