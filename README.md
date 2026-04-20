# SQS Connector

A high-level Kotlin library for working with Amazon SQS, built on top of
the [AWS SDK for Kotlin](https://github.com/awslabs/aws-sdk-kotlin) and [Arrow-kt](https://arrow-kt.io/).

## Features

- **Coroutines Based**: Fully asynchronous API using Kotlin Coroutines.
- **Functional Error Handling**: Uses Arrow's `Either` and `Ior` for explicit error handling.
- **Graceful Draining**: Terminate message consumption gracefully, ensuring in-flight messages are processed before
  shutdown.
- **Automatic Visibility Extension**: Automatically extends the visibility timeout of messages currently being
  processed.
- **Batch Processing**: Built-in support for efficient batch sending and deleting of messages.
- **Strongly Typed**: Type-safe representations for Queue URLs, Names, and Receipt Handles.

## Installation

Add the dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.github.jckoenen:sqs-connector:VERSION")
}
```

## Usage

### Initializing SqsConnector

The `SqsConnector` is the main entry point for SQS operations.

```kotlin
val sqsClient = SqsClient { region = "us-east-1" }
val connector = SqsConnector(sqsClient)
```

### Consuming Messages

The library provides a powerful `consume` abstraction that allows to focus on handling messages, abstracting all the plumbing of SQS.
Messages are automatically received from the queue, the visibility timeout is (by default) extended while they are in flight, and finally the action is taken.
For better efficiency, messages are batched as needed.

Additionally, the returned `Flow` is `Drainable`, which can be used to gracefully stop after processing messages that are already in flight. 

```kotlin
// setup consumer
val consumer = MessageConsumer.Individual { message ->
  println("Processing: ${message.content}")
  return MessageConsumer.Action.DeleteMessage(message)
}

// connect consumer to a queue
val drainControl = connector.consumeIn(Queue.Name("my-queue"), consumer, parentScope = this)

// stop receiving but handle all inflight messages
val completed = withTimeoutOrNull(10.seconds) { drainControl.drainAndJoin() }

// or cancel
if (completed == null) drainControl.job.cancel()
```

### Receiving Messages as Flow

To use the full flexibilty of the `Flow` APIs, use `SqsConnector.receive`. The returned flow is also `Drainable`

```kotlin
// create the stream of messages
val messages: DrainableFlow<List<Message<String>>> = connector.receive(Queue.Name("my-queue"))

// handle messages
val control = messages.concatMap { it.asFlow() }
    .onEach(::println)
    .onEach { message -> connector.deleteMessages(message.queue.url, nonEmptyListOf(message)) }
    .drainable()
    .launchWithDrainControl(scope)

// stop receiving but handle all inflight messages
control.drainAndJoin()
```

### Error Handling

Instead of throwing exceptions, this library leverages Arrow's functional types to make error states explicit. Most
operations return an `Either<Failure, Success>`, forcing you to handle potential errors at compile time. For batch
operations, we use a `BatchResult` (based on Arrow's `Ior`), which gracefully handles "partial success" scenarios—common
in SQS when some messages in a batch fail while others succeed.

> [!INFO]
> When unavoidable, Exceptions are raised when a usage error is detected.

### Sending Messages

```kotlin
val queueUrl = Queue.Url("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
val messages = nonEmptyListOf(
    OutboundMessage("First message"),
    OutboundMessage("Second message")
)

val result = connector.sendMessages(queueUrl, messages)

result.fold(
    { failures ->
        // Handle failures (Map<SendMessagesFailure, Nel<FailedBatchEntry>>)
        println("Some or all messages failed: $failures")
    },
    { success ->
        // Handle total success (Nel<OutboundMessage>)
        println("All messages sent successfully: $success")
    },
    { failures, success ->
        // Handle partial success
        println("Successfully sent: $success")
        println("Failed: $failures")
    }
)
```

### Prerequisites

- JDK 17 or higher
- Docker (for running tests via LocalStack)

### Running Tests

Tests use [Testcontainers](https://www.testcontainers.org/) with LocalStack to provide a real SQS environment.

```bash
./gradlew check
```

### Applying Formatting

```bash
./gradlew spotlessApply
```

### Releasing

```bash
op run --env-file="deploy-secrets.env" -- ./gradlew publishToMavenCentral --no-daemon
```
