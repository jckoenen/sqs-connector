package io.github.jckoenen.sqs.testinfra

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.listQueues
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.impl.kotlin.KotlinSqsConnector
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.listeners.AfterProjectListener
import io.kotest.core.listeners.BeforeProjectListener
import io.kotest.core.test.TestScope
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.testcontainers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName

internal object SqsContainerExtension : BeforeProjectListener, AfterProjectListener {
    private val NON_WORD_CHARS = """\W""".toRegex()
    private val container =
        DockerImageName.parse("localstack/localstack:4.12").let(::LocalStackContainer).withServices("sqs")

    private val scope =
        CoroutineScope(
            CoroutineExceptionHandler { _, ex ->
                LoggerFactory.getLogger(SqsContainerExtension::class.java)
                    .atError()
                    .setCause(ex)
                    .log("Scope closed due to uncaught exception")
            })

    private val client =
        scope.async(start = CoroutineStart.LAZY) {
            withContext(Dispatchers.IO) { container.start() }

            val client = SqsClient.fromEnvironment {
                region = container.region
                endpointUrl = Url.parse(container.endpoint.toString())
                credentialsProvider = StaticCredentialsProvider {
                    accessKeyId = container.accessKey
                    secretAccessKey = container.secretKey
                }
            }
            // localstack is NOT immediately ready, wait for one call to succeed
            eventually(10.seconds) { client.listQueues { maxResults = 1 } }

            client
        }

    override suspend fun beforeProject() {
        // start container eagerly on CI to get better test timings
        if (System.getenv("CI").isNullOrBlank()) return
        client.await()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override suspend fun afterProject() {
        super.afterProject()
        if (!client.isCompleted) return
        withContext(Dispatchers.IO) {
            client.getCompleted().close()
            container.stop()
        }
    }

    suspend fun newConnector(clientOverride: SqsClient? = null) = KotlinSqsConnector(clientOverride ?: client.await())

    fun TestScope.queueName(): Queue.Name =
        testCase.descriptor
            .ids()
            .joinToString(separator = "_") { id -> id.value.substringAfterLast(".") }
            .replace(NON_WORD_CHARS, "_")
            .takeLast(64)
            .let(Queue::Name)

    fun TestScope.fifoQueueName() = Queue.Name(queueName().value + ".fifo")
}
