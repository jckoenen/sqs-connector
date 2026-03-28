package io.github.jckoenen.sqs

/** Represents a failure that occurred during message processing. */
public interface Failure {
    /** Additional context for the failure, used for logging or monitoring. */
    public val customTags: Map<String, Any>

    /** A human-readable description of the failure. */
    public val message: String
}

internal fun Failure.allTags() =
    customTags +
        mapOf("sqs.failure.kind" to (this::class.simpleName ?: "Unnamed failure"), "sqs.failure.message" to message)
