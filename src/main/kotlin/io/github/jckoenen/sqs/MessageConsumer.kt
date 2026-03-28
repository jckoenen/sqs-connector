package io.github.jckoenen.sqs

import arrow.core.Nel
import kotlin.time.Duration

/** A consumer that processes messages from an SQS queue. */
public sealed interface MessageConsumer {
    /**
     * Configuration for the message consumer.
     *
     * @property parallelism the number of messages to process in parallel
     */
    public interface Configuration {
        public val parallelism: Int
    }

    /** The result of processing a message, indicating the next action to be taken by the dispatcher. */
    public sealed interface Action : MessageBound {
        /**
         * Deletes the message from the queue.
         *
         * @property message the message to delete
         */
        public data class DeleteMessage(val message: Message<*>) : Action, MessageBound by message

        /**
         * Retries the message after a certain duration.
         *
         * @property message the message to retry
         * @property backoffDuration the duration to wait before the message becomes visible again
         */
        public data class RetryBackoff(val message: Message<*>, val backoffDuration: Duration) :
            Action, MessageBound by message

        /**
         * Moves the message to the Dead Letter Queue (DLQ).
         *
         * @property message the message to move
         */
        public data class MoveMessageToDlq(val message: Message<String>) : Action, MessageBound by message
    }

    /** The configuration for this consumer. */
    public val configuration: Configuration

    /** A consumer that processes messages individually. */
    public interface Individual : MessageConsumer {
        /**
         * Handles a single message.
         *
         * @param message the message to handle
         * @return the [Action] to take after processing
         */
        public suspend fun handle(message: Message<String>): Action

        public companion object
    }

    /** A consumer that processes messages in batches. */
    public interface Batch : MessageConsumer {
        /**
         * Handles a batch of messages.
         *
         * IMPORTANT: Every input message MUST result in an action. Failing to do so with active visibility management
         * will cause the message to be stuck until the consumer is stopped completely.
         *
         * @param messages the messages to handle
         * @return a list of [Action]s, one for each input message
         */
        public suspend fun handle(messages: Nel<Message<String>>): Nel<Action>

        public companion object
    }
}

/**
 * Convenience function to create a [MessageConsumer.Batch].
 *
 * @param parallelism the number of batches to process in parallel. Defaults to 1.
 * @param handleFn the suspendable function to handle a batch of messages and produce corresponding actions.
 * @return a batch message consumer configured with the specified parallelism and message handling function.
 */
public operator fun MessageConsumer.Batch.Companion.invoke(
    parallelism: Int = 1,
    handleFn: suspend (Nel<Message<String>>) -> Nel<MessageConsumer.Action>
): MessageConsumer.Batch =
    object : MessageConsumer.Batch {
        override suspend fun handle(messages: Nel<Message<String>>): Nel<MessageConsumer.Action> = handleFn(messages)

        override val configuration: MessageConsumer.Configuration
            get() =
                object : MessageConsumer.Configuration {
                    override val parallelism: Int
                        get() = parallelism
                }
    }

/**
 * Convenience function to create a [MessageConsumer.Individual].
 *
 * @param parallelism the number of messages to process in parallel. Defaults to 1.
 * @param handleFn the suspendable function to handle a batch of messages and produce corresponding actions.
 * @return a batch message consumer configured with the specified parallelism and message handling function.
 */
public operator fun MessageConsumer.Individual.Companion.invoke(
    parallelism: Int = 1,
    handleFn: suspend (Message<String>) -> MessageConsumer.Action
): MessageConsumer.Individual =
    object : MessageConsumer.Individual {
        override suspend fun handle(message: Message<String>): MessageConsumer.Action = handleFn(message)

        override val configuration: MessageConsumer.Configuration
            get() =
                object : MessageConsumer.Configuration {
                    override val parallelism: Int
                        get() = parallelism
                }
    }
