package io.github.jckoenen.sqs.flow

import arrow.core.toNonEmptySetOrNull
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.MessageBound
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.allTags
import io.github.jckoenen.sqs.utils.asTags
import io.github.jckoenen.sqs.utils.id
import io.github.jckoenen.sqs.utils.putAll
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

internal class VisibilityManager(
    private val connector: SqsConnector,
    private val extensionDuration: Duration,
    extensionOffset: Duration = 3.seconds,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val activeBatches = BatchMap<Message.ReceiptHandle>()
    private val interval = maxOf(500.milliseconds, extensionDuration - extensionOffset)

    init {
        check(interval < extensionDuration) {
            "Extension interval $interval must be shorter than extension duration $extensionDuration"
        }
    }

    suspend fun startTracking(messages: Collection<MessageBound>, parentScope: CoroutineScope) {
        messages.groupBy(MessageBound::queue).forEach { (queue, byQueue) ->
            val ref = activeBatches.register(byQueue.map(MessageBound::receiptHandle))
            parentScope.schedule(interval, queue, ref)

            logger
                .atDebug()
                .addKeyValue("visibilityBatch.id", ref.identityCode())
                .addKeyValue("visibilityBatch.size", byQueue.size)
                .addKeyValue("refresh.interval", interval.toString())
                .addKeyValue("refresh.duration", extensionDuration.toString())
                .putAll(queue.id().asTags())
                .log("Started automatic visibility management")
        }
    }

    suspend fun stopTracking(message: MessageBound) {
        activeBatches.remove(message.receiptHandle)
    }

    private fun CoroutineScope.schedule(
        delay: Duration,
        queue: Queue,
        reference: BatchMap.BatchRef<Message.ReceiptHandle>,
    ): Job = launch {
        select {
            async { reference.awaitEmpty() }.onAwait {}
            onTimeout(delay) {}
        }

        val messages = reference.items()
        val log =
            logger
                .atDebug()
                .addKeyValue("sqs.visibilityBatch.id", reference.identityCode())
                .addKeyValue("sqs.visibilityBatch.size", messages?.size ?: 0)

        if (messages == null) {
            log.log("No messages left to extend visibility")
            return@launch
        }
        log.log("Extending visibility")

        connector
            .extendMessageVisibility(queue.url, messages, extensionDuration)
            .leftOrNull()
            .orEmpty()
            .flatMap { (cause, affected) -> affected.map { cause to it } }
            .onEach { (cause, failure) ->
                val level = if (isAlreadyDeleted(failure)) Level.DEBUG else Level.WARN
                logger
                    .atLevel(level)
                    .putAll(cause.allTags())
                    .addKeyValue("sqs.failure.ref", failure.reference)
                    .addKeyValue("sqs.failure.code", failure.code)
                    .addKeyValue("sqs.failure.message", failure.errorMessage)
                    .addKeyValue("sqs.failure.senderFault", failure.senderFault)
                    .log("Couldn't extend visibility for message. Will NOT retry")
            }
            .forEach { (_, failure) -> activeBatches.remove(failure.reference) }

        this@schedule.schedule(interval, queue, reference)
    }

    private fun isAlreadyDeleted(entry: SqsConnector.FailedBatchEntry<*>) =
        entry.code == "InvalidParameterValueException" ||
            entry.code == "ReceiptHandleIsInvalid" ||
            entry.code == "MessageNotInflight"

    private data class BatchMap<T>(private val batches: MutableMap<T, BatchRef<T>> = mutableMapOf()) {
        /**
         * Prevents concurrent modification of [batches]
         *
         * There are three concurrent actors operating on it:
         * 1. Upstream emitting a new batch, registering items using [register]
         * 2. Downstream emitting an item, causing un-registration using [remove]
         * 3. The manager itself after an item failed to be extended, also using [remove]
         */
        private val mutex = Mutex()

        /**
         * Enforces batch registration to go through the mutex and associates it with that same mutex
         *
         * The items reference in the ref is itself mutable, meaning a call to [remove] will remove it from our global
         * [batches] map, but also from the individual ref it was contained in
         */
        suspend fun register(batch: Collection<T>): BatchRef<T> = mutex.withLock {
            val inner = batch.toMutableSet()
            val ref = BatchRef(inner, mutex)
            inner.forEach { k -> batches[k] = ref }
            ref
        }

        /** Removes the element from the global reference AND from whatever [BatchRef] it was associated to. */
        suspend fun remove(element: T) = mutex.withLock { batches.remove(element)?.remove(element) }

        data class BatchRef<T>(private val items: MutableSet<T>, private val mutex: Mutex) {
            private val done = CompletableDeferred<Unit>()

            suspend fun items() = mutex.withLock(action = items::toNonEmptySetOrNull)

            fun remove(item: T) {
                if (items.remove(item) && items.isEmpty()) {
                    done.complete(Unit)
                }
            }

            suspend fun awaitEmpty() {
                done.await()
            }
        }
    }

    private fun Any.identityCode() = System.identityHashCode(this).toHexString()
}
