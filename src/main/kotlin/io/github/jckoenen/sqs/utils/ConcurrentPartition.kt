package io.github.jckoenen.sqs.utils

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.plus

internal inline fun <T, R> Flow<T>.concurrentPartition(
    concurrency: Int,
    crossinline partitionBy: suspend (T) -> Any,
    crossinline processingFn: suspend (T) -> R
): Flow<R> = channelFlow {
    check(concurrency > 0) { "Concurrency must be > 0, got $concurrency" }
    val partitions = List(concurrency) { Channel<T>() }
    val workers = partitions.mapIndexed { index, channel ->
        channel
            .consumeAsFlow()
            .map(processingFn)
            .onEach(::send)
            .launchIn(this + CoroutineName("partition-worker-$index"))
    }

    this@concurrentPartition.collect {
        val discriminator = partitionBy(it)
        val target = discriminator.hashCode().ushr(1) % concurrency
        partitions[target].send(it)
    }

    partitions.forEach { it.close() }
    workers.joinAll()
}
