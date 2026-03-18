package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.leftIor
import arrow.core.raise.either
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.getQueueAttributes
import aws.sdk.kotlin.services.sqs.model.QueueAttributeName
import io.github.jckoenen.sqs.FifoQueueImpl
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.QueueImpl
import io.github.jckoenen.sqs.SqsFailure
import io.github.jckoenen.sqs.impl.RedrivePolicy
import kotlinx.serialization.json.Json

private val DEFAULT_JSON = Json { ignoreUnknownKeys = true }
private const val GET_ATTRIBUTES_OP = "SQS.GetQueueAttributes"

internal suspend fun SqsClient.getDlq(url: Queue.Url) = either {
    val attributes =
        execute(unknownFailure(GET_ATTRIBUTES_OP, url)) {
                getQueueAttributes {
                    queueUrl = url.value
                    attributeNames = listOf(QueueAttributeName.RedrivePolicy)
                }
            }
            .bind()
            .attributes
    val rawPolicy = attributes?.get(QueueAttributeName.RedrivePolicy) ?: return@either null

    val arn =
        DEFAULT_JSON.decodeFromString<RedrivePolicy>(rawPolicy)
            .targetArn
            .mapLeft { SqsFailure.UnknownFailure(GET_ATTRIBUTES_OP, url.leftIor(), IllegalArgumentException(it)) }
            .bind()

    if (arn.name.designatesFifo()) {
        FifoQueueImpl(arn.name, arn.toUrl(config), null)
    } else {
        QueueImpl(arn.name, arn.toUrl(config), null)
    }
}

internal fun buildAttributes(
    isFifo: Boolean? = null,
    redrivePolicy: RedrivePolicy? = null,
) = buildMap {
    infix fun QueueAttributeName.from(value: String?) {
        if (value != null) put(this, value)
    }

    QueueAttributeName.FifoQueue from isFifo?.takeIf { it }?.toString()
    QueueAttributeName.RedrivePolicy from redrivePolicy?.let(DEFAULT_JSON::encodeToString)
}
