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
package run.ratchet.spring.boot.it.nativepostgresql;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.stereotype.Component;

/** Public persisted job targets shared by the JVM control and native executable. */
@Component
public class NativePostgresqlJobs {

  private static final int RETRY_FAILURES = 2;
  private static final AtomicInteger STATIC_REFERENCE_EXECUTIONS = new AtomicInteger();
  private static final AtomicReference<String> WRAPPER_OBSERVATION = new AtomicReference<>();

  private final AtomicInteger directExecutions = new AtomicInteger();
  private final AtomicInteger boundReferenceExecutions = new AtomicInteger();
  private final AtomicInteger persistenceExecutions = new AtomicInteger();
  private final AtomicInteger retryAttempts = new AtomicInteger();
  private final AtomicInteger recurringExecutions = new AtomicInteger();
  private final AtomicReference<NativePostgresqlPayload> receivedPayload = new AtomicReference<>();
  private final AtomicReference<CountDownLatch> blockingStarted = new AtomicReference<>();
  private final AtomicReference<CountDownLatch> blockingRelease = new AtomicReference<>();
  private final AtomicBoolean blockingCompleted = new AtomicBoolean();

  public void directJob() {
    directExecutions.incrementAndGet();
  }

  public void boundReferenceJob() {
    boundReferenceExecutions.incrementAndGet();
  }

  public static void staticReferenceJob() {
    STATIC_REFERENCE_EXECUTIONS.incrementAndGet();
  }

  public static void wrapperJob(String value, WrapperCapture capture) {
    WRAPPER_OBSERVATION.set(value + ":" + capture.label() + ":" + capture.sequence());
  }

  public void persistenceJob() {
    persistenceExecutions.incrementAndGet();
  }

  public void retryJob() {
    int attempt = retryAttempts.incrementAndGet();
    if (attempt <= RETRY_FAILURES) {
      throw new IllegalStateException("intentional retry attempt " + attempt);
    }
  }

  public void recurringJob() {
    recurringExecutions.incrementAndGet();
  }

  public void jsonbJob(NativePostgresqlPayload payload) {
    receivedPayload.set(payload);
  }

  public void blockingJob() throws InterruptedException {
    CountDownLatch started = blockingStarted.get();
    CountDownLatch release = blockingRelease.get();
    if (started == null || release == null) {
      throw new IllegalStateException("blocking job was not prepared");
    }
    started.countDown();
    if (!release.await(30, TimeUnit.SECONDS)) {
      throw new IllegalStateException("blocking job was not released within 30 seconds");
    }
    blockingCompleted.set(true);
  }

  void reset() {
    directExecutions.set(0);
    boundReferenceExecutions.set(0);
    STATIC_REFERENCE_EXECUTIONS.set(0);
    WRAPPER_OBSERVATION.set(null);
    persistenceExecutions.set(0);
    retryAttempts.set(0);
    recurringExecutions.set(0);
    receivedPayload.set(null);
    blockingStarted.set(null);
    blockingRelease.set(null);
    blockingCompleted.set(false);
  }

  int directExecutions() {
    return directExecutions.get();
  }

  int boundReferenceExecutions() {
    return boundReferenceExecutions.get();
  }

  int staticReferenceExecutions() {
    return STATIC_REFERENCE_EXECUTIONS.get();
  }

  String wrapperObservation() {
    return WRAPPER_OBSERVATION.get();
  }

  int persistenceExecutions() {
    return persistenceExecutions.get();
  }

  int retryAttempts() {
    return retryAttempts.get();
  }

  int recurringExecutions() {
    return recurringExecutions.get();
  }

  NativePostgresqlPayload receivedPayload() {
    return receivedPayload.get();
  }

  void prepareBlockingJob() {
    blockingStarted.set(new CountDownLatch(1));
    blockingRelease.set(new CountDownLatch(1));
    blockingCompleted.set(false);
  }

  boolean awaitBlockingStarted(Duration timeout) throws InterruptedException {
    CountDownLatch started = blockingStarted.get();
    return started != null && started.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  void releaseBlockingJob() {
    CountDownLatch release = blockingRelease.get();
    if (release != null) {
      release.countDown();
    }
  }

  boolean blockingCompleted() {
    return blockingCompleted.get();
  }

  /** Record argument captured by the inline wrapper and round-tripped through PostgreSQL JSON-B. */
  public record WrapperCapture(String label, int sequence) {}
}
