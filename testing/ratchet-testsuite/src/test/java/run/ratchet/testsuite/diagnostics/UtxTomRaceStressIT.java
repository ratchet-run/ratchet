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
package run.ratchet.testsuite.diagnostics;

import static org.junit.jupiter.api.Assertions.fail;

import jakarta.enterprise.concurrent.ManagedExecutorService;
import jakarta.inject.Inject;
import jakarta.transaction.UserTransaction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.naming.InitialContext;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestDataManipulator;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Stress coverage for the GlassFish/Payara {@code TransactionalInterceptorBase}
 * TransactionOperationsManager race behind an observed {@code Operation not allowed} CI flake.
 *
 * <p>The container bug is an unsynchronized {@code preexistingTransactionOperationsManager}
 * instance field on a CDI interceptor instance shared by concurrent invocations of an
 * {@code @ApplicationScoped} bean. If one request-thread store call restores another executor
 * thread's nested {@code NOT_ALLOWED} TransactionOperationsManager, the request frame is poisoned
 * and the next direct {@link UserTransaction#begin()} fails.
 *
 * <p>Two methods, two purposes: the {@link Disabled} diagnostic reproduces the container bug itself
 * via a bare {@code UserTransaction} (run it manually to check whether a container upgrade fixed
 * the race); the enabled regression method drives {@link TestDataManipulator} under the same load
 * and guards ratchet's test infrastructure against reintroducing bare UserTransaction calls on
 * paths that share beans with background work.
 */
class UtxTomRaceStressIT extends BaseRatchetIT {

  private static final Logger log = Logger.getLogger(UtxTomRaceStressIT.class.getName());
  private static final String DEFAULT_MANAGED_EXECUTOR_JNDI =
      "java:comp/DefaultManagedExecutorService";
  private static final int RACER_COUNT = 3;
  private static final long STRESS_NANOS = TimeUnit.SECONDS.toNanos(20);
  private static final long REGRESSION_NANOS = TimeUnit.SECONDS.toNanos(6);
  private static final long STOP_WAIT_MILLIS = 300;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private TomRaceNestedCaller nested;

  @Inject private TestDataManipulator dataManipulator;

  @Inject private UserTransaction utx;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addStoreInfrastructure()
        .addClasses(TomRaceNestedCaller.class)
        .addBeansXml()
        .build();
  }

  @Test
  void dataManipulatorUnderNestedTransactionalLoad_neverTripsUtxGate() throws Exception {
    JobEntity job = persistFailedJob();
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicLong pokes = new AtomicLong();
    AtomicLong racerExceptions = new AtomicLong();
    long iterations = 0;
    List<Future<?>> futures = new ArrayList<>();

    ManagedExecutorService executor = InitialContext.doLookup(DEFAULT_MANAGED_EXECUTOR_JNDI);
    for (int i = 0; i < RACER_COUNT; i++) {
      futures.add(executor.submit(() -> raceUntilStopped(running, pokes, racerExceptions)));
    }

    try {
      long deadline = System.nanoTime() + REGRESSION_NANOS;
      while (System.nanoTime() < deadline) {
        iterations++;
        try {
          dataManipulator.setJobUpdatedAt(job.getId(), Instant.now().minusSeconds(60));
        } catch (RuntimeException e) {
          if (hasTomPoisoningCause(e)) {
            fail(
                "Manipulator tripped the UserTransaction gate after "
                    + iterations
                    + " iterations and "
                    + pokes.get()
                    + " background pokes — a bare UserTransaction call is back on a raceable path",
                e);
          }
          throw e;
        }
      }

      long finalIterations = iterations;
      log.info(
          () ->
              "Manipulator survived "
                  + finalIterations
                  + " iterations under "
                  + pokes.get()
                  + " background pokes; swallowed racer exceptions="
                  + racerExceptions.get());
    } finally {
      stopRacers(running, futures);
    }
  }

  @Test
  @Disabled(
      "Reproduces a Payara/GlassFish TransactionalInterceptorBase TOM race (container bug -"
          + " upstream report pending); run manually to verify container fixes")
  void requestThreadUtxBegin_shouldNotInheritNestedExecutorTom() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicLong pokes = new AtomicLong();
    AtomicLong racerExceptions = new AtomicLong();
    AtomicLong requestIterations = new AtomicLong();
    List<Future<?>> futures = new ArrayList<>();

    ManagedExecutorService executor = InitialContext.doLookup(DEFAULT_MANAGED_EXECUTOR_JNDI);
    for (int i = 0; i < RACER_COUNT; i++) {
      futures.add(executor.submit(() -> raceUntilStopped(running, pokes, racerExceptions)));
    }

    try {
      long deadline = System.nanoTime() + STRESS_NANOS;
      while (System.nanoTime() < deadline) {
        long iteration = requestIterations.incrementAndGet();
        jobCrudStore.findById(UUID.randomUUID());

        try {
          utx.begin();
        } catch (IllegalStateException e) {
          if (isTomPoisoningSymptom(e)) {
            running.set(false);
            stopRacers(running, futures);
            String message =
                "Reproduced TransactionalInterceptorBase TOM poisoning after "
                    + iteration
                    + " request iterations and "
                    + pokes.get()
                    + " background pokes; swallowed racer exceptions="
                    + racerExceptions.get();
            log.warning(message);
            fail(message, e);
          }
          throw e;
        }

        utx.commit();
      }

      log.info(
          () ->
              "No TransactionalInterceptorBase TOM poisoning observed after "
                  + requestIterations.get()
                  + " request iterations and "
                  + pokes.get()
                  + " background pokes; swallowed racer exceptions="
                  + racerExceptions.get());
    } finally {
      stopRacers(running, futures);
    }
  }

  private JobEntity persistFailedJob() {
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.FAILED);
    job.setPriority(JobPriority.NORMAL);
    job.setScheduledTime(Instant.now().minusSeconds(5));
    job.setPayload(JobPayloadFactory.noop());
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setAttempts(0);
    job.setMaxRetries(0);
    job.setLastError("boom");
    return jobCrudStore.save(job);
  }

  private void raceUntilStopped(
      AtomicBoolean running, AtomicLong pokes, AtomicLong racerExceptions) {
    while (running.get()) {
      try {
        nested.poke();
        pokes.incrementAndGet();
      } catch (Exception e) {
        racerExceptions.incrementAndGet();
      }
    }
  }

  private static boolean isTomPoisoningSymptom(IllegalStateException e) {
    return e.getMessage() != null && e.getMessage().contains("Operation not allowed");
  }

  private static boolean hasTomPoisoningCause(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (cause instanceof IllegalStateException
          && cause.getMessage() != null
          && cause.getMessage().contains("Operation not allowed")) {
        return true;
      }
    }
    return false;
  }

  private static void stopRacers(AtomicBoolean running, List<Future<?>> futures) {
    running.set(false);
    for (Future<?> future : futures) {
      future.cancel(true);
    }
    try {
      Thread.sleep(STOP_WAIT_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
