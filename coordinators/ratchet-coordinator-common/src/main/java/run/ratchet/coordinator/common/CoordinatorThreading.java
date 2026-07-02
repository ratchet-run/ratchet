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
package run.ratchet.coordinator.common;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Single source of threads for the cluster coordinators. Every coordinator obtains one of these and
 * asks it for its long-running loop threads and its bounded dispatch pool; no coordinator
 * constructs a {@link Thread} or {@link ExecutorService} directly. That is what lets the
 * architecture test ban raw thread creation everywhere except this class.
 *
 * <p>Two modes, chosen explicitly at construction:
 *
 * <ul>
 *   <li><b>Managed</b> ({@link #managed(String)}): resolves a {@link ThreadFactory} from {@code
 *       java:comp/DefaultManagedThreadFactory} by direct JNDI lookup, the same well-known name
 *       Jakarta Concurrency 3.0+ binds in every compliant Jakarta EE 10+ container. The reference
 *       is resolved lazily on first use and cached. <b>A JNDI miss throws</b> — managed mode never
 *       silently degrades to unmanaged threads, because a coordinator that escaped onto raw threads
 *       in production would be invisible to the container's thread governance.
 *   <li><b>Standalone</b> ({@link #standalone(String)}): an explicit opt-in for plain-SE, demos,
 *       and tests. Uses a plain daemon {@link ThreadFactory}. This is never reached by a JNDI miss;
 *       the caller must ask for it by name.
 * </ul>
 *
 * <p><b>Why direct JNDI rather than {@code @Resource}.</b> Some containers (notably Payara) do not
 * honor {@code @Resource(lookup=...)} on library beans and fail deployment trying to bind {@code
 * java:comp/env/<class>/<field>}. Looking up the well-known name directly is portable across every
 * compliant runtime, exactly as {@code DefaultExecutorProvider} does for managed executors.
 *
 * <p><b>Interrupt contract.</b> A managed-thread-factory thread may be interrupted by the container
 * before the coordinator's own {@code close()} runs (Jakarta Concurrency 3.0 §3.1.4). Loop handlers
 * must not swallow {@link InterruptedException}; on interrupt they must restore the flag and exit
 * promptly. Every long-running loop checks {@link Thread#isInterrupted()} as a second exit
 * condition alongside its {@code closed} flag.
 */
public final class CoordinatorThreading {

  /** Well-known Jakarta Concurrency 3.0+ managed thread factory name. */
  static final String MANAGED_THREAD_FACTORY_JNDI = "java:comp/DefaultManagedThreadFactory";

  private final boolean managed;
  private final String threadNamePrefix;
  private final AtomicLong threadCounter = new AtomicLong();

  // Managed mode only. Resolved lazily on first use and cached.
  private final String jndiName;
  private volatile ThreadFactory resolvedFactory;

  private CoordinatorThreading(
      boolean managed, String threadNamePrefix, String jndiName, ThreadFactory presetFactory) {
    this.managed = managed;
    this.threadNamePrefix = threadNamePrefix;
    this.jndiName = jndiName;
    this.resolvedFactory = presetFactory;
  }

  /**
   * Container mode. Resolves {@code java:comp/DefaultManagedThreadFactory} lazily and throws on a
   * JNDI miss. {@code threadNamePrefix} names the loop threads (the container may rename them).
   */
  public static CoordinatorThreading managed(String threadNamePrefix) {
    return managed(threadNamePrefix, MANAGED_THREAD_FACTORY_JNDI);
  }

  /**
   * Container mode. Resolves the supplied managed thread factory JNDI name lazily and throws on a
   * JNDI miss. {@code threadNamePrefix} names the loop threads (the container may rename them).
   */
  public static CoordinatorThreading managed(String threadNamePrefix, String jndiName) {
    return new CoordinatorThreading(
        true, threadNamePrefix, requireJndiName(jndiName), /* presetFactory= */ null);
  }

  /**
   * Explicit standalone opt-in for plain-SE, demos, and tests. Plain daemon threads; never reached
   * by a JNDI miss.
   */
  public static CoordinatorThreading standalone(String threadNamePrefix) {
    return new CoordinatorThreading(
        false, threadNamePrefix, /* jndiName= */ null, daemonThreadFactory(threadNamePrefix));
  }

  /**
   * Container mode against a caller-supplied factory. For tests that exercise the managed path with
   * a stubbed {@link ThreadFactory} without standing up a JNDI context.
   */
  public static CoordinatorThreading managed(String threadNamePrefix, ThreadFactory factory) {
    return new CoordinatorThreading(true, threadNamePrefix, /* jndiName= */ null, factory);
  }

  /** True when this instance routes through a container-managed thread factory. */
  public boolean isManaged() {
    return managed;
  }

  /**
   * Create (but do not start) a thread for a long-running coordinator loop, named {@code
   * <prefix>-<role>-<n>}. The caller starts and tracks it for interrupt-and-join on close.
   */
  public Thread newLoopThread(String role, Runnable body) {
    Thread thread = threadFactory().newThread(body);
    thread.setName(threadNamePrefix + "-" + role + "-" + threadCounter.incrementAndGet());
    thread.setDaemon(true);
    return thread;
  }

  /**
   * A bounded dispatch pool: {@code threads} core/max workers over an {@link ArrayBlockingQueue} of
   * {@code queueCapacity}, backed by this instance's thread factory. The {@link
   * ThreadPoolExecutor.DiscardOldestPolicy} drops the oldest queued wakeup when the bound is hit —
   * wakeups are advisory and a fresher one supersedes a stale one, so shedding the oldest is the
   * right pressure-relief. The coordinator owns the returned pool and shuts it down on close. A
   * bounded queue replaces the unbounded one in {@code Executors.newFixedThreadPool}, which is a
   * latent OOM under sustained wakeup pressure.
   */
  public ExecutorService newDispatchPool(String role, int threads, int queueCapacity) {
    int workers = Math.max(1, threads);
    int capacity = Math.max(1, queueCapacity);
    ThreadFactory base = threadFactory();
    AtomicLong dispatchCounter = new AtomicLong();
    ThreadFactory named =
        runnable -> {
          Thread thread = base.newThread(runnable);
          thread.setName(threadNamePrefix + "-" + role + "-" + dispatchCounter.incrementAndGet());
          thread.setDaemon(true);
          return thread;
        };
    return new ThreadPoolExecutor(
        workers,
        workers,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(capacity),
        named,
        new ThreadPoolExecutor.DiscardOldestPolicy());
  }

  private ThreadFactory threadFactory() {
    ThreadFactory factory = resolvedFactory;
    if (factory == null) {
      synchronized (this) {
        factory = resolvedFactory;
        if (factory == null) {
          factory = resolveManagedFactory();
          resolvedFactory = factory;
        }
      }
    }
    return factory;
  }

  private ThreadFactory resolveManagedFactory() {
    try {
      Object bound = new InitialContext().lookup(jndiName);
      if (!(bound instanceof ThreadFactory)) {
        throw new IllegalStateException(
            "CoordinatorThreading expected "
                + jndiName
                + " to be a java.util.concurrent.ThreadFactory");
      }
      return (ThreadFactory) bound;
    } catch (NamingException e) {
      throw new IllegalStateException(
          "CoordinatorThreading could not resolve "
              + jndiName
              + " from JNDI. Configure ratchet.coordinator.thread-factory-jndi if this deployment"
              + " exposes the managed thread factory under a different name; if you are running"
              + " outside a Jakarta EE 10+ container, build the coordinator with"
              + " CoordinatorThreading.standalone(...).",
          e);
    }
  }

  private static String requireJndiName(String jndiName) {
    String value = Objects.requireNonNull(jndiName, "jndiName").trim();
    if (value.isEmpty()) {
      throw new IllegalArgumentException("jndiName must not be blank");
    }
    return value;
  }

  private static ThreadFactory daemonThreadFactory(String prefix) {
    AtomicLong counter = new AtomicLong();
    return runnable -> {
      Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }
}
