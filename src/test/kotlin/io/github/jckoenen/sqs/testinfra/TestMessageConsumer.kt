package io.github.jckoenen.sqs.testinfra

import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.MessageConsumer
import io.github.jckoenen.sqs.MessageConsumer.Action
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update

internal abstract class TestMessageConsumer private constructor(parallelism: Int = 1) : MessageConsumer.Individual {
    override val configuration =
        object : MessageConsumer.Configuration {
            override val parallelism: Int
                get() = parallelism
        }

    private val _seen = MutableStateFlow(emptyList<Message<String>>())
    val seen
        get() = _seen.asStateFlow()

    override suspend fun handle(message: Message<String>): Action {
        _seen.update { it + message }
        return doHandle(message)
    }

    protected abstract suspend fun doHandle(message: Message<String>): Action

    companion object {
        fun create(parallelism: Int = 1, handleFn: suspend (Message<String>) -> Action = Action::DeleteMessage) =
            object : TestMessageConsumer(parallelism) {
                override suspend fun doHandle(message: Message<String>): Action = handleFn(message)
            }
    }
}
