/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.tck.api;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.SignalDecision;

/**
 * Static job-body helpers used by the seed contracts. Exists so contract task bodies are <em>method
 * references</em> ({@code TckJobs::noop}, {@code TckJobs::throwIntentional}, …) rather than inline
 * lambdas.
 *
 * <h2>Why method references and not inline lambdas?</h2>
 *
 * <p>Implementations are free to inspect the bytecode of submitted task bodies for things like
 * idempotency-key derivation or bean-resolver hints. Real-world bytecode analyzers reject several
 * patterns that an inline lambda happily generates:
 *
 * <ul>
 *   <li>An empty {@code () -> {}} body has no method calls to anchor analysis.
 *   <li>An inline {@code () -> { throw ...; }} contains an {@code ATHROW} opcode that some
 *       analyzers do not handle.
 *   <li>Inline lambdas that capture non-serializable locals (e.g. {@link CountDownLatch}) cannot
 *       round-trip through any portable serializer.
 * </ul>
 *
 * <p>Routing the task body through a static method on this class side-steps all three: the lambda's
 * bytecode is a single {@code INVOKESTATIC}, captures nothing, and serializes trivially.
 *
 * <h2>Process-wide state, by design</h2>
 *
 * <p>The blocking-task and chain-recording helpers use process-static state. Job lambdas
 * deserialize on the worker side and have no view of test-method instance fields, so a
 * process-static rendezvous is the only mechanism that survives the serialization boundary. Tests
 * must call {@link #resetAll()} (typically from {@code @AfterEach}) so state from one test does not
 * leak into the next.
 */
public final class TckJobs {

  private static final AtomicReference<CountDownLatch> STARTED_LATCH = new AtomicReference<>();
  private static final AtomicReference<CountDownLatch> RELEASE_LATCH = new AtomicReference<>();
  private static final ConcurrentLinkedQueue<String> CHAIN_EVENTS = new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<String> WORKFLOW_BRANCH_EVENTS =
      new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<String> SIGNAL_DECISIONS =
      new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<String> RAW_SIGNAL_PAYLOADS =
      new ConcurrentLinkedQueue<>();

  private TckJobs() {}

  /**
   * No-op task body. Calls {@link Thread#yield()} so the bytecode contains at least one method
   * invocation, which keeps the lambda body inspectable for analyzers that walk the called method's
   * body in addition to the lambda's own body.
   */
  public static void noop() {
    Thread.yield();
  }

  /** Always-failing task body. Used by retry / failure-path contracts. */
  public static void throwIntentional() {
    throw new IllegalStateException("intentional TCK failure");
  }

  /**
   * Blocking task body. Tests must call {@link #beginBlocking()} <em>before</em> submitting a job
   * that uses this method as its body, then await the returned started-latch, then call {@link
   * #release()}.
   */
  public static void blockUntilReleased() throws InterruptedException {
    CountDownLatch started = STARTED_LATCH.get();
    CountDownLatch release = RELEASE_LATCH.get();
    if (started == null || release == null) {
      throw new IllegalStateException("beginBlocking() must be called before blockUntilReleased()");
    }
    started.countDown();
    release.await();
  }

  /** Installs fresh started/release latches and returns the started latch the test awaits on. */
  public static CountDownLatch beginBlocking() {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    STARTED_LATCH.set(started);
    RELEASE_LATCH.set(release);
    return started;
  }

  /** Releases the blocking task body so it can complete. */
  public static void release() {
    CountDownLatch r = RELEASE_LATCH.get();
    if (r != null) {
      r.countDown();
    }
  }

  /** Chain-step task bodies for {@link AbstractSimpleWorkflowContract}. */
  public static void recordStepA() {
    CHAIN_EVENTS.add("step-A");
  }

  public static void recordStepB() {
    CHAIN_EVENTS.add("step-B");
  }

  public static void recordStepC() {
    CHAIN_EVENTS.add("step-C");
  }

  /** Always-matching workflow predicate for exclusive-branch contracts. */
  public static boolean workflowConditionMatches(JobResult<?> ignored) {
    return true;
  }

  /** Preferred and sibling branch bodies for {@link AbstractExclusiveWorkflowContract}. */
  public static void recordPreferredWorkflowBranch() {
    WORKFLOW_BRANCH_EVENTS.add("preferred");
  }

  public static void recordSiblingWorkflowBranch() {
    WORKFLOW_BRANCH_EVENTS.add("sibling");
  }

  /** Signal-waiting task body that records the delivered decision visible through JobContext. */
  public static void recordSignalDecision() {
    SignalDecision decision = JobContext.current().signalPayload(SignalDecision.class);
    SIGNAL_DECISIONS.add(
        decision == null
            ? "null"
            : decision.outcome()
                + ":"
                + decision.payload(String.class)
                + ":"
                + decision.rejectionReason());
  }

  /**
   * Signal-waiting task body that records the delivered <em>raw</em> payload's observed runtime
   * category and value through {@link JobContext}. Used by {@link AbstractSignalPayloadContract} to
   * pin the JSON-native round-trip of a {@code deliverSignal(.., Serializable)} payload across
   * stores. The recorded token is {@code "<category>:<value>"} where category is the JSON-native
   * shape ({@code String}, {@code Boolean}, {@code Number}, {@code List}, {@code Map}) rather than
   * the original concrete class, which is intentionally not reconstructed.
   */
  public static void recordRawSignalPayload() {
    RAW_SIGNAL_PAYLOADS.add(
        describeRawPayload(JobContext.current().signalPayload(Serializable.class)));
  }

  private static String describeRawPayload(Serializable payload) {
    if (payload == null) {
      return "null";
    }
    if (payload instanceof String s) {
      return "String:" + s;
    }
    if (payload instanceof Boolean b) {
      return "Boolean:" + b;
    }
    // JSON-B maps a bare JSON number to a Number (commonly a BigDecimal), so the contract
    // compares by value, not by concrete type.
    if (payload instanceof Number n) {
      return "Number:" + n;
    }
    // Sort entries/elements so the rendering is deterministic regardless of the deserialized
    // collection impl's iteration order.
    if (payload instanceof Map<?, ?> m) {
      TreeMap<String, String> sorted = new TreeMap<>();
      m.forEach((k, v) -> sorted.put(String.valueOf(k), String.valueOf(v)));
      return "Map:" + sorted;
    }
    if (payload instanceof List<?> l) {
      return "List:" + l;
    }
    return "Other(" + payload.getClass().getName() + "):" + payload;
  }

  /** Snapshot of recorded raw signal payload tokens in observation order. */
  public static List<String> rawSignalPayloads() {
    return List.copyOf(RAW_SIGNAL_PAYLOADS);
  }

  /** Snapshot of recorded chain events in observation order. */
  public static List<String> chainEvents() {
    return List.copyOf(CHAIN_EVENTS);
  }

  /** Snapshot of workflow branch bodies invoked in observation order. */
  public static List<String> workflowBranchEvents() {
    return List.copyOf(WORKFLOW_BRANCH_EVENTS);
  }

  /** Snapshot of recorded signal decisions in observation order. */
  public static List<String> signalDecisions() {
    return List.copyOf(SIGNAL_DECISIONS);
  }

  /** Resets every piece of process-static state. Call from {@code @AfterEach}. */
  public static void resetAll() {
    STARTED_LATCH.set(null);
    RELEASE_LATCH.set(null);
    CHAIN_EVENTS.clear();
    WORKFLOW_BRANCH_EVENTS.clear();
    SIGNAL_DECISIONS.clear();
    RAW_SIGNAL_PAYLOADS.clear();
  }
}
