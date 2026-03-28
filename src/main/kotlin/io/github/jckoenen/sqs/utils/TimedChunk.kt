package io.github.jckoenen.sqs.utils

import arrow.atomic.Atomic
import arrow.core.Nel
import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.wrapAsNonEmptyListOrThrow
import kotlin.time.Duration
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

// The number of chunks can be waiting on downstream before exerting backpressure on upstream
private const val HIGH_WATERMARK = 3
// The number of chunks waiting on downstream to start reading upstream again
private const val LOW_WATERMARK = 1

internal fun <T> Flow<T>.chunked(size: Int, timeout: Duration): Flow<Nel<T>> = channelFlow {
    // bridge downstream suspension to the disconnected upstream consumer
    val waitingChunks = MutableStateFlow(0)

    // protects buffer from concurrent access (timer OR upstream emission)
    val bufferMutex = Mutex()
    var buffer = ArrayList<T>(size)

    // always possible to send to downstream so that timer cancellation can never lose elements
    // additionally, it ensures that pushing is always fast - that's important because of the mutex
    val downstream = Channel<Nel<T>>(capacity = Channel.BUFFERED)
    val upstream = this@chunked

    // if the item is not null, it's added to the buffer if the buffer is full, flush it downstream;
    // if the item is null, flush the buffer if it's not empty
    // returns true if the buffer was flushed, false otherwise
    suspend fun push(item: T?): Boolean =
        bufferMutex.withLock {
            item?.let(buffer::add)

            if (buffer.isNotEmpty() && (item == null || buffer.size == size)) {
                @OptIn(PotentiallyUnsafeNonEmptyOperation::class) val bufferNel = buffer.wrapAsNonEmptyListOrThrow()

                require(downstream.trySend(bufferNel).isSuccess)
                waitingChunks.update(Int::inc)
                buffer = ArrayList(size)
                true
            } else {
                false
            }
        }

    // currently active timer
    val timer = Atomic<Job?>()

    suspend fun stopTimer() = timer.getAndSet(null)?.cancelAndJoin()
    fun startTimer() =
        launch(context = CoroutineName("chunk-timer"), start = CoroutineStart.LAZY) {
            while (true) {
                delay(timeout)
                // if there was nothing to flush, we can stop polling
                // as soon as there is a new item coming in from upstream, we will be restarted
                val flushed = withContext(NonCancellable) { push(null) }
                if (!flushed) stopTimer()
            }
        }

    // returns true if the timer was started through this call
    fun ensureTimerStarted(): Boolean {
        if (timer.get() != null) return false

        val new = startTimer()
        // if the timer was null before, set it to the new job and start it
        // if the timer was something else, do nothing
        val didLaunch = timer.compareAndSet(null, new) && new.start()
        if (!didLaunch) new.cancel()
        return didLaunch
    }

    val emitter =
        downstream
            .consumeAsFlow()
            .onEach(::send)
            .onEach { waitingChunks.update(Int::dec) }
            .launchIn(this + CoroutineName("downstream-emitter"))

    upstream.collect { item ->
        // Check currently waiting chunks: if downstream is slow, suspend until it catches up
        if (waitingChunks.value >= HIGH_WATERMARK) {
            waitingChunks.firstOrNull { it <= LOW_WATERMARK }
        }

        if (push(item)) {
            // The buffer was just flushed, so it's empty until we see another item
            // => no need to keep the timer running
            stopTimer()
        } else {
            // The buffer was not flushed, so ensure there is a running timer
            ensureTimerStarted()
        }
    }
    // upstream completed, so
    stopTimer() // don't need the timer any more
    push(null) // ensure the buffer is flushed
    downstream.close() // close the channel for writing
    emitter.join() // and wait until downstream has consumed it
}
