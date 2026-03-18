package io.github.jckoenen.sqs.utils

import arrow.core.Nel
import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.wrapAsNonEmptyListOrThrow
import kotlin.reflect.KClass

private typealias ClassMap<T> = Map<KClass<out T>, List<T>>

@JvmInline
internal value class TypedMap<T : Any> private constructor(private val underlying: ClassMap<T>) :
    ClassMap<T> by underlying {

    @OptIn(PotentiallyUnsafeNonEmptyOperation::class)
    inline fun <reified K : T> get(): Nel<K>? = underlying[K::class]?.filterIsInstance<K>()?.wrapAsNonEmptyListOrThrow()

    inline fun <reified K : T, R> onMatching(f: (Nel<K>) -> R): R? = get<K>()?.let(f)

    companion object {
        fun <T : Any> Collection<T>.byType() = TypedMap(groupBy { it::class })
    }
}
