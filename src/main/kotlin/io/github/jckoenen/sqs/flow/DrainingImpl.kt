package io.github.jckoenen.sqs.flow

import arrow.atomic.Atomic
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.internal.FusibleFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

internal fun <T> Flow<T>.drainableImpl(): DrainableFlow<T> =
    channelFlow {
            val upstream = onEach(::send).launchIn(this)
            val drain = currentCoroutineContext()[DrainControlElement]

            if (drain != null) {
                drain.head.set(upstream)
            } else {
                LoggerFactory.getLogger(DrainableFlow::class.java)
                    .warn("Drainable flow did not have a drain signal attached")
            }
        }
        .let(::DrainableFlowImpl)

internal fun <T> drainableImpl(f: suspend FlowCollector<T>.() -> Unit) = flow(f).drainableImpl()

@OptIn(InternalCoroutinesApi::class)
@JvmInline
internal value class DrainableFlowImpl<T> private constructor(private val delegate: FusibleFlow<T>) :
    DrainableFlow<T>, FusibleFlow<T> by delegate {

    internal constructor(delegate: Flow<T>) : this(delegate as? FusibleFlow<T> ?: delegate.buffer(0) as FusibleFlow<T>)

    override suspend fun collect(collector: FlowCollector<T>) = delegate.collect(collector)

    override fun launchWithDrainControl(scope: CoroutineScope): DrainControl {
        val element = DrainControlElement()
        val job = scope.launch(element) { collect() }
        return DrainControlImpl(job, element)
    }
}

private class DrainControlElement(val head: Atomic<Job?> = Atomic()) : CoroutineContext.Element {
    override val key: CoroutineContext.Key<*>
        get() = DrainControlElement

    companion object : CoroutineContext.Key<DrainControlElement>
}

private data class DrainControlImpl(
    override val job: Job,
    private val element: DrainControlElement,
) : DrainControl {
    override fun drain() {
        element.head.getAndSet(null)?.cancel()
    }
}
