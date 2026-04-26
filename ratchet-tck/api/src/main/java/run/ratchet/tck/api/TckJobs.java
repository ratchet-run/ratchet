package run.ratchet.tck.api;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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
    if (started != null) {
      started.countDown();
    }
    if (release != null) {
      release.await();
    }
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

  /** Snapshot of recorded chain events in observation order. */
  public static List<String> chainEvents() {
    return List.copyOf(CHAIN_EVENTS);
  }

  /** Resets every piece of process-static state. Call from {@code @AfterEach}. */
  public static void resetAll() {
    STARTED_LATCH.set(null);
    RELEASE_LATCH.set(null);
    CHAIN_EVENTS.clear();
  }
}
