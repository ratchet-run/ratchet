package run.ratchet.testsuite.concurrency;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Job target that records the thread it executes on. Static state (mirroring {@code SimpleJob}) is
 * read directly by the in-container IT.
 *
 * <p>Each invocation captures {@link Thread#isVirtual()} and the thread description, briefly sleeps
 * so concurrent invocations overlap, and tracks peak concurrency. The IT asserts every invocation
 * ran on a virtual thread.
 */
public final class VirtualThreadProbeJob {

  private static final AtomicInteger INVOCATIONS = new AtomicInteger();
  private static final AtomicInteger CONCURRENT = new AtomicInteger();
  private static final AtomicInteger PEAK_CONCURRENT = new AtomicInteger();
  private static final List<Boolean> VIRTUAL_FLAGS = new CopyOnWriteArrayList<>();
  private static final Set<String> THREAD_NAMES = ConcurrentHashMap.newKeySet();

  private VirtualThreadProbeJob() {}

  public static void execute() {
    int inFlight = CONCURRENT.incrementAndGet();
    PEAK_CONCURRENT.accumulateAndGet(inFlight, Math::max);
    try {
      Thread current = Thread.currentThread();
      VIRTUAL_FLAGS.add(current.isVirtual());
      THREAD_NAMES.add(current.toString());
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      INVOCATIONS.incrementAndGet();
    } finally {
      CONCURRENT.decrementAndGet();
    }
  }

  public static int invocations() {
    return INVOCATIONS.get();
  }

  public static int peakConcurrency() {
    return PEAK_CONCURRENT.get();
  }

  public static boolean allRanOnVirtualThreads() {
    return !VIRTUAL_FLAGS.isEmpty() && VIRTUAL_FLAGS.stream().allMatch(Boolean::booleanValue);
  }

  public static Set<String> threadNames() {
    return THREAD_NAMES;
  }

  public static void reset() {
    INVOCATIONS.set(0);
    CONCURRENT.set(0);
    PEAK_CONCURRENT.set(0);
    VIRTUAL_FLAGS.clear();
    THREAD_NAMES.clear();
  }
}
