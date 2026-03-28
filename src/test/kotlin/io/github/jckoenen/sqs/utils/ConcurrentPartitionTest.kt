package io.github.jckoenen.sqs.utils

import arrow.core.identity
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.common.KotestInternal
import io.kotest.core.names.TestNameBuilder
import io.kotest.core.spec.style.FreeSpec
import io.kotest.core.spec.style.TestXMethod
import io.kotest.core.spec.style.scopes.FreeSpecContainerScope
import io.kotest.core.spec.style.scopes.FreeSpecTerminalScope
import io.kotest.inspectors.forAll
import io.kotest.matchers.collections.shouldBeSortedBy
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList

private val CONCURRENCY_LEVELS = listOf(1, 3, 10)

class ConcurrentPartitionTest : FreeSpec({

    fun <T : Any> Flow<T>.partition(
        concurrency: Int,
        partitionBy: suspend (T) -> Any = { it },
        processingFn: suspend (T) -> T = { it },
    ) = concurrentPartition(concurrency, partitionBy, processingFn)

    @OptIn(KotestInternal::class)
    context(scope: FreeSpecContainerScope)
    suspend infix operator fun String.rem(test: suspend FreeSpecTerminalScope.(Int) -> Unit) {
        CONCURRENCY_LEVELS.forEach { concurrency ->
            val name = TestNameBuilder.builder("[concurrency = $concurrency] $this")
                .build()
            scope.registerTest(
                name = name,
                xmethod = TestXMethod.NONE,
                config = null
            ) { FreeSpecTerminalScope(this).test(concurrency) }
        }

    }


    "Flow.concurrentPartition should" - {
        "reject concurrency <= 0" {
            shouldThrow<IllegalStateException> {
                emptyFlow<Int>().partition(concurrency = 0).toList()
            }.message shouldBe "Concurrency must be > 0, got 0"

            shouldThrow<IllegalStateException> {
                emptyFlow<Int>().partition(concurrency = -1).toList()
            }.message shouldBe "Concurrency must be > 0, got -1"
        }

        "complete immediately on empty upstream" % { c ->
            val result = emptyFlow<Int>().partition(concurrency = c).toList()
            result shouldBe emptyList()
        }

        "process all items" % { c ->
            val count = 100_000
            val result = generateSequence(0, Int::inc)
                .asFlow()
                .take(count)
                .partition(concurrency = c)
                .toList()

            result shouldHaveSize count
        }

        "preserve ordering within each partition" % { c ->
            val groups = listOf("a", "b", "c")
            val items = groups.flatMap { g ->
                List(20) { i -> g to i }
            }

            val result = items.asFlow()
                .partition(concurrency = c, partitionBy = { (group, _) -> group })
                .toList()

            result shouldContainExactlyInAnyOrder items

            result.groupBy(keySelector = { it.first }, valueTransform = { it.second })
                .forAll { (_, v) -> v shouldBeSortedBy ::identity }
        }

        "propagate upstream errors to the collector" % { c ->
            val upstream = flow {
                emit(1)
                emit(2)
                throw RuntimeException("upstream boom")
            }

            val thrown = shouldThrow<RuntimeException> {
                upstream.partition(concurrency = c).toList()
            }
            thrown.message shouldBe "upstream boom"
        }

        "stop upstream when downstream uses take(n)" % { c ->
            val upstream = flow {
                for (i in 1..1000) emit(i)
            }

            val result = upstream
                .partition(concurrency = c)
                .take(5)
                .toList()

            result.size shouldBe 5
        }

        "propagate processingFn errors to the collector" % { c ->
            val thrown = shouldThrow<RuntimeException> {
                (1..10).asFlow()
                    .partition(
                        concurrency = c,
                        processingFn = { if (it == 5) throw RuntimeException("worker boom") else it },
                    )
                    .toList()
            }
            thrown.message shouldBe "worker boom"
        }

        "propagate partitionBy errors to the collector" % { c ->
            val thrown = shouldThrow<RuntimeException> {
                (1..10).asFlow()
                    .partition(
                        concurrency = c,
                        partitionBy = { if (it == 3) throw RuntimeException("partition boom") else it },
                    )
                    .toList()
            }
            thrown.message shouldBe "partition boom"
        }
    }
})
