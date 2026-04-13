package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class OptimisticLockRetryTest {

  private static JobEntity newEntity(long id, JobStatus status, int version) {
    JobEntity e = new JobEntity();
    e.setId(id);
    e.setStatus(status);
    e.setVersion(version);
    return e;
  }

  @Test
  void happyPath_firstSaveSucceeds_returnsSaved() {
    JobEntity reloaded = newEntity(1L, JobStatus.PENDING, 1);
    JobEntity saved = newEntity(1L, JobStatus.RUNNING, 2);
    AtomicInteger reloadCount = new AtomicInteger();
    AtomicInteger mutateCount = new AtomicInteger();
    AtomicInteger saveCount = new AtomicInteger();

    JobEntity result =
        OptimisticLockRetry.retryWithReload(
            1L,
            () -> {
              reloadCount.incrementAndGet();
              return reloaded;
            },
            e -> {
              mutateCount.incrementAndGet();
              e.setStatus(JobStatus.RUNNING);
            },
            e -> {
              saveCount.incrementAndGet();
              return saved;
            });

    assertSame(saved, result);
    assertEquals(1, reloadCount.get());
    assertEquals(1, mutateCount.get());
    assertEquals(1, saveCount.get());
  }

  @Test
  void retryOnConflict_eventuallySucceeds() {
    JobEntity reloaded = newEntity(2L, JobStatus.PENDING, 1);
    JobEntity saved = newEntity(2L, JobStatus.RUNNING, 3);
    AtomicInteger saveAttempt = new AtomicInteger();

    Function<JobEntity, JobEntity> flakySave =
        e -> {
          int attempt = saveAttempt.incrementAndGet();
          if (attempt < 3) {
            throw new RatchetOptimisticLockException("version mismatch on attempt " + attempt);
          }
          return saved;
        };

    JobEntity result =
        OptimisticLockRetry.retryWithReload(
            3, 2L, () -> reloaded, e -> e.setStatus(JobStatus.RUNNING), flakySave);

    assertSame(saved, result);
    assertEquals(3, saveAttempt.get());
  }

  @Test
  void reloadReturnsNull_throwsImmediately() {
    RatchetOptimisticLockException ex =
        assertThrows(
            RatchetOptimisticLockException.class,
            () ->
                OptimisticLockRetry.retryWithReload(
                    99L,
                    () -> null,
                    e -> {
                      throw new AssertionError("mutate should not run");
                    },
                    e -> {
                      throw new AssertionError("save should not run");
                    }));
    assertTrue(ex.getMessage().contains("no longer exists"));
  }

  @Test
  void reloadedEntityTerminal_throwsWithoutMutating() {
    JobEntity canceled = newEntity(3L, JobStatus.CANCELED, 5);
    AtomicInteger mutateCount = new AtomicInteger();
    AtomicInteger saveCount = new AtomicInteger();

    RatchetOptimisticLockException ex =
        assertThrows(
            RatchetOptimisticLockException.class,
            () ->
                OptimisticLockRetry.retryWithReload(
                    3L,
                    () -> canceled,
                    e -> mutateCount.incrementAndGet(),
                    e -> {
                      saveCount.incrementAndGet();
                      return e;
                    }));

    assertTrue(ex.getMessage().contains("terminal state CANCELED"));
    assertEquals(0, mutateCount.get(), "mutate must not run on a terminal-state reload");
    assertEquals(0, saveCount.get(), "save must not run on a terminal-state reload");
  }

  @Test
  void maxAttemptsExhausted_throwsLastException() {
    JobEntity reloaded = newEntity(4L, JobStatus.PENDING, 1);
    AtomicInteger saveCount = new AtomicInteger();

    RatchetOptimisticLockException ex =
        assertThrows(
            RatchetOptimisticLockException.class,
            () ->
                OptimisticLockRetry.retryWithReload(
                    3,
                    4L,
                    () -> reloaded,
                    e -> e.setStatus(JobStatus.RUNNING),
                    e -> {
                      int attempt = saveCount.incrementAndGet();
                      throw new RatchetOptimisticLockException("miss " + attempt);
                    }));

    assertEquals(3, saveCount.get(), "should attempt the save maxAttempts times");
    assertTrue(ex.getMessage().startsWith("miss 3"), "should propagate the LAST miss exception");
  }

  @Test
  void interruptDuringBackoff_restoresInterruptFlagAndWraps() {
    JobEntity reloaded = newEntity(5L, JobStatus.PENDING, 1);

    Function<JobEntity, JobEntity> alwaysConflict =
        e -> {
          throw new RatchetOptimisticLockException("always miss");
        };

    Thread.interrupted(); // clear any stale flag
    RatchetOptimisticLockException ex =
        assertThrows(
            RatchetOptimisticLockException.class,
            () ->
                OptimisticLockRetry.retryWithReload(
                    5,
                    5L,
                    () -> reloaded,
                    e -> Thread.currentThread().interrupt(),
                    alwaysConflict));

    assertTrue(ex.getMessage().contains("Retry interrupted"));
    assertTrue(Thread.interrupted(), "interrupt flag must be restored on the calling thread");
  }

  @Test
  void nonOptimisticException_propagatesWithoutRetry() {
    JobEntity reloaded = newEntity(6L, JobStatus.PENDING, 1);
    AtomicInteger saveCount = new AtomicInteger();

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                OptimisticLockRetry.retryWithReload(
                    3,
                    6L,
                    () -> reloaded,
                    e -> e.setStatus(JobStatus.RUNNING),
                    e -> {
                      saveCount.incrementAndGet();
                      throw new IllegalStateException("unrelated failure");
                    }));

    assertEquals(1, saveCount.get(), "non-optimistic exceptions must not trigger retry");
    assertEquals("unrelated failure", ex.getMessage());
  }

  @Test
  void maxAttemptsLessThanOne_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OptimisticLockRetry.retryWithReload(
                0, 7L, () -> newEntity(7L, JobStatus.PENDING, 1), e -> {}, e -> e));
  }
}
