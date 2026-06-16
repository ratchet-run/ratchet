---
title: Durable LLM & Agent Workflows
description: Run LLM calls and multi-step agent workflows as persisted, retrying Ratchet jobs in a Jakarta EE app, with langchain4j-cdi supplying the model client.
---

# Durable LLM & Agent Workflows

Calling a language model from a request thread is a trap. The call is slow, it fails in ways ordinary code does not (a 429 from the provider, a socket timeout, a model that is briefly overloaded), and an agent rarely makes just one call: it retrieves context, calls the model, runs a tool, calls the model again. Do that inline and a single restart loses everything in flight, and a provider hiccup surfaces as a failed HTTP request to your user.

This is the problem durable execution solves, and it is what Ratchet already does for any other background work. Ratchet is not an AI framework and ships no model client. It is the layer underneath: it persists the call before it runs, retries it with backoff, trips a circuit breaker when the provider is down, and resumes a half-finished multi-step workflow after a crash. You bring the model client. On Jakarta EE the natural choice is [langchain4j-cdi](https://github.com/langchain4j/langchain4j-cdi), which exposes a [LangChain4j](https://docs.langchain4j.dev) AI service as an injectable CDI bean, the same programming model Ratchet uses.

::: tip Verified
The Java on this page compiles against `ratchet-api` `0.1.1-SNAPSHOT`, `langchain4j-cdi-portable-ext` `1.3.3`, and `langchain4j-bedrock` `1.0.0-beta5`. It is not a full runnable application (wiring it up needs a Jakarta EE server and AWS credentials), but the API usage is real, not illustrative pseudocode.
:::

## What you wire together

```xml
<!-- Ratchet: the durable execution engine + a store -->
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet</artifactId>
  <version>0.1.1-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-postgresql</artifactId>
  <version>0.1.1-SNAPSHOT</version>
</dependency>

<!-- langchain4j-cdi: AI services as CDI beans on WildFly, Liberty, Payara, GlassFish -->
<dependency>
  <groupId>dev.langchain4j.cdi</groupId>
  <artifactId>langchain4j-cdi-portable-ext</artifactId>
  <version>1.3.3</version>
</dependency>

<!-- A model provider; Amazon Bedrock here, but any LangChain4j provider works -->
<dependency>
  <groupId>dev.langchain4j</groupId>
  <artifactId>langchain4j-bedrock</artifactId>
  <version>1.0.0-beta5</version>
</dependency>
```

## Define the AI service

A LangChain4j AI service is a plain interface. The `@RegisterAIService` annotation makes langchain4j-cdi register it as a CDI bean you can inject anywhere, including inside a Ratchet job.

```java
import dev.langchain4j.cdi.spi.RegisterAIService;
import dev.langchain4j.service.SystemMessage;
import dev.langchain4j.service.UserMessage;
import dev.langchain4j.service.V;

@RegisterAIService(chatModelName = "doc-assistant")
public interface DocAssistant {

  @SystemMessage("You are a precise technical summarizer. Answer in {{language}}.")
  String summarize(@UserMessage String document, @V("language") String language);
}
```

## Provide the model

`chatModelName` points at a CDI bean that produces the `ChatModel`. Build it once and let the container manage it:

```java
import dev.langchain4j.model.bedrock.BedrockChatModel;
import dev.langchain4j.model.chat.ChatModel;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;

@ApplicationScoped
public class ChatModelProducer {

  @Produces
  @Named("doc-assistant")
  @ApplicationScoped
  public ChatModel docAssistantModel() {
    return BedrockChatModel.builder()
        .modelId("us.anthropic.claude-haiku-4-5-20251001-v1:0")
        .build();
  }
}
```

`BedrockChatModel` uses the default AWS credential chain and region resolution, so the same code runs against any model on Bedrock by changing the `modelId`. If you would rather configure the model with properties than code, langchain4j-cdi can build it from MicroProfile Config instead. See the [langchain4j-cdi docs](https://github.com/langchain4j/langchain4j-cdi).

## Run the model call as a durable job

Here is the part Ratchet exists for. The request thread enqueues the work and returns; the model call runs on a worker, persisted, with retries and a timeout. A 429 or a dropped connection no longer fails the user's request. It just retries with backoff.

```java
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.UUID;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;

@ApplicationScoped
public class DocumentIntake {

  @Inject JobSchedulerService scheduler;

  @Inject DocAssistant assistant;

  /** The request thread returns immediately; the model call runs as a persisted, retrying job. */
  public JobHandle enqueueSummary(UUID documentId) {
    return scheduler
        .enqueue(() -> summarize(documentId))
        .withMaxRetries(5)
        .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
        .withTimeout(Duration.ofMinutes(2))
        .withTags("llm", "summarize")
        .submit();
  }

  /** Runs inside the job. Ratchet resolves this bean from CDI; only documentId is serialized. */
  public void summarize(UUID documentId) {
    String text = loadDocument(documentId);
    String summary = assistant.summarize(text, "English");
    storeSummary(documentId, summary);
  }
}
```

The lambda passed to `enqueue` is a method call on a CDI bean. Ratchet serializes only the arguments (`documentId`) and resolves the bean again from CDI when the job runs, so the injected `DocAssistant` is live at execution time, not captured at submission. Exhaust the retries and the job lands in the dead-letter queue with its error, instead of vanishing.

Exponential backoff is the right default for a model provider: rate limits and brief overloads clear on their own, and backing off is what they are asking you to do.

## Chain the steps of an agent workflow

Agents are multi-step, and the steps fail independently. Model each step as its own job and chain them. A failure in step three does not discard the results of steps one and two, which are already persisted.

```java
scheduler.enqueue(() -> retrieveContext(queryId))
    .thenOnSuccess(() -> answerWithModel(queryId))
    .thenOnSuccess(() -> indexAnswer(queryId))
    .thenOnFailure(() -> flagForReview(queryId))
    .submit();
```

Each step is a separate persisted job with its own retry budget. If `answerWithModel` keeps failing, Ratchet retries that step alone and the retrieved context is not thrown away and re-fetched.

## Pause for a human, then resume

Some agent actions should not happen without sign-off: sending the email it drafted, filing the ticket, running the refund. Ratchet has a waiting state for exactly this: a job can wait for a signal, and an approval (from a UI, a webhook, a Slack action) delivers that signal to resume it. The job holds no thread while it waits. See [Workflows](../concepts/workflows.md) and the signal APIs on [`JobSchedulerService`](../api-reference/job-scheduler-service.md) for the delivery contract.

## Why run it through Ratchet

You could call the model inline, or stand up a separate durable-execution service. Ratchet's argument is the same one it makes everywhere else: it lives inside the Jakarta EE server you already run. No second orchestrator to operate, no extra datastore, no separate programming model. The model client is a CDI bean, the job is a CDI bean, and the work survives a restart because it was written to your database before it ran.

## Next steps

- [Retry strategies](../concepts/retry-strategies.md) -- tune backoff and max attempts per job
- [Workflows](../concepts/workflows.md) -- chaining, branching, and signal-waiting in depth
- [Circuit breakers](../advanced/circuit-breakers.md) -- trip out a model provider that is down
- [Quickstart](../getting-started/quickstart.md) -- get a first job running
