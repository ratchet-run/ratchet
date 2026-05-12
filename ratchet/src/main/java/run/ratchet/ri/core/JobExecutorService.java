package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobLoggerFactory;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStore;

/**
 * Executes jobs on the configured executor. A permit must be acquired before calling {@link
 * #execute}; it is released automatically on completion. Jakarta EE deployments should provide a
 * managed executor through {@link ExecutorProvider}.
 */
@ApplicationScoped
public class JobExecutorService {

  private static final Logger log = Logger.getLogger(JobExecutorService.class);
  private final ThreadPoolManager threadPoolManager;
  private final JobTimeoutHandler timeoutHandler;
  private final ExecutorProvider executorProvider;
  private final JobStore jobStore;
  private final ResourcePermitService resourcePermitService;
  private final PostExecutionHandler postExecutionHandler;
  private final NodeIdentityProvider nodeIdProvider;
  private final ExecutionObserver executionObserver;
  private final PreExecutionValidator preExecutionValidator;
  private final BeanResolver beanResolver;
  private final RetryPolicy retryPolicy;
  private final ResilienceStrategy resilienceStrategy;
  private final ErrorSanitizer errorSanitizer;
  private final ClassPolicy classPolicy;
  private final JobLoggerFactory jobLoggerFactory;
  private final ResultPersistenceStrategy resultPersistenceStrategy;
  private final JobAuthorizationPolicy authorizationPolicy;
  private final PayloadSerializer payloadSerializer;
  private final PollerScheduler pollerScheduler;
  private final Clock clock;
  private final Object submissionLock = new Object();
  private final AtomicBoolean acceptingExecutions = new AtomicBoolean(true);
  private final Set<TrackingFutureTask> activeFutures = ConcurrentHashMap.newKeySet();

  protected JobExecutorService() {
    this.threadPoolManager = null;
    this.timeoutHandler = null;
    this.executorProvider = null;
    this.jobStore = null;
    this.resourcePermitService = null;
    this.postExecutionHandler = null;
    this.nodeIdProvider = null;
    this.executionObserver = null;
    this.preExecutionValidator = null;
    this.beanResolver = null;
    this.retryPolicy = null;
    this.resilienceStrategy = null;
    this.errorSanitizer = null;
    this.classPolicy = null;
    this.jobLoggerFactory = null;
    this.resultPersistenceStrategy = null;
    this.authorizationPolicy = null;
    this.payloadSerializer = null;
    this.pollerScheduler = null;
    this.clock = null;
  }

  @Inject
  public JobExecutorService(
      ThreadPoolManager threadPoolManager,
      JobTimeoutHandler timeoutHandler,
      ExecutorProvider executorProvider,
      JobStore jobStore,
      ResourcePermitService resourcePermitService,
      PostExecutionHandler postExecutionHandler,
      NodeIdentityProvider nodeIdProvider,
      ExecutionObserver executionObserver,
      PreExecutionValidator preExecutionValidator,
      BeanResolver beanResolver,
      RetryPolicy retryPolicy,
      ResilienceStrategy resilienceStrategy,
      ErrorSanitizer errorSanitizer,
      ClassPolicy classPolicy,
      PollerScheduler pollerScheduler,
      JobLoggerFactory jobLoggerFactory,
      ResultPersistenceStrategy resultPersistenceStrategy,
      JobAuthorizationPolicy authorizationPolicy,
      PayloadSerializer payloadSerializer,
      Clock clock) {
    this.threadPoolManager = threadPoolManager;
    this.timeoutHandler = timeoutHandler;
    this.executorProvider = executorProvider;
    this.jobStore = jobStore;
    this.resourcePermitService = resourcePermitService;
    this.postExecutionHandler = postExecutionHandler;
    this.nodeIdProvider = nodeIdProvider;
    this.executionObserver = executionObserver;
    this.preExecutionValidator = preExecutionValidator;
    this.beanResolver = beanResolver;
    this.retryPolicy = retryPolicy;
    this.resilienceStrategy = resilienceStrategy;
    this.errorSanitizer = errorSanitizer;
    this.classPolicy = classPolicy;
    this.pollerScheduler = pollerScheduler;
    this.jobLoggerFactory = jobLoggerFactory;
    this.resultPersistenceStrategy = resultPersistenceStrategy;
    this.authorizationPolicy = authorizationPolicy;
    this.payloadSerializer = payloadSerializer;
    this.clock = clock;
  }

  private static void cancelTimeoutHandles(
      AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef) {
    JobTimeoutHandler.TimeoutHandles handles = handlesRef.getAndSet(null);
    if (handles != null) {
      handles.cancel();
    }
  }

  public ExecutionResult execute(JobEntity job) {
    JobExecutionType jobType = job.getJobType();
    AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef = new AtomicReference<>();
    Callable<Void> callable = createPermitAwareRunner(jobType, handlesRef, task -> task.init(job));
    return execute(job.getId(), job.getTimeoutSec(), callable, handlesRef);
  }

  public ExecutionResult execute(JobClaimDto claim) {
    JobExecutionType jobType = claim.jobType();
    AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef = new AtomicReference<>();
    Callable<Void> callable =
        createPermitAwareRunner(jobType, handlesRef, task -> task.initFromClaim(claim));
    return execute(claim.id(), claim.timeoutSec(), callable, handlesRef);
  }

  private ExecutionResult execute(
      UUID jobId,
      int timeoutSec,
      Callable<Void> callable,
      AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef) {
    Instant executionStartTime = effective().instant();
    TrackingFutureTask task;
    try {
      task = prepareTask(callable);
    } catch (RejectedExecutionException e) {
      return ExecutionResult.rejected(e);
    }

    try {
      JobTimeoutHandler.TimeoutHandles handles =
          scheduleWatchdog(jobId, timeoutSec, task, executionStartTime);
      handlesRef.set(handles);
      submitToExecutor(task);
      if (task.isDone()) {
        cancelTimeoutHandles(handlesRef);
      }
      return ExecutionResult.success(task);
    } catch (RejectedExecutionException e) {
      cancelTimeoutHandles(handlesRef);
      task.cancel(true);
      return ExecutionResult.rejected(e);
    }
  }

  /**
   * Non-destructive drain: blocks until all currently-executing jobs reach a terminal state, or
   * until {@code timeout} elapses. Unlike {@link #shutdownActiveExecutions()}, does NOT cancel
   * running tasks. Pair with {@link DrainController#setDraining(boolean) DrainController
   * .setDraining(true)} on entry to prevent new work from arriving during the wait.
   *
   * <p>Primary consumer: TCK runtime adapters that need a non-destructive between-tests reset
   * without tearing down the application-scoped scheduler bean.
   *
   * @return {@code true} if the executor became idle within the timeout; {@code false} otherwise
   */
  public boolean awaitIdle(Duration timeout) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (!activeFutures.isEmpty()) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        break;
      }
      TrackingFutureTask snapshot;
      try {
        snapshot = activeFutures.iterator().next();
      } catch (NoSuchElementException raceLost) {
        break;
      }
      boolean exited = snapshot.awaitRunnerExit(Math.min(remainingNanos, 50_000_000L));
      if (!exited && System.nanoTime() >= deadlineNanos) {
        break;
      }
    }
    return activeFutures.isEmpty();
  }

  public int shutdownActiveExecutions() {
    synchronized (submissionLock) {
      acceptingExecutions.set(false);
      for (TrackingFutureTask task : activeFutures) {
        task.cancel(true);
      }
    }

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!activeFutures.isEmpty() && System.nanoTime() < deadlineNanos) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        break;
      }
      try {
        boolean exited =
            activeFutures.iterator().next().awaitRunnerExit(Math.min(remainingNanos, 10_000_000L));
        if (!exited && System.nanoTime() >= deadlineNanos) {
          break;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    if (!activeFutures.isEmpty()) {
      log.warnf("Shutdown proceeding with %s active job execution(s)", activeFutures.size());
    }
    return activeFutures.size();
  }

  private Callable<Void> createPermitAwareRunner(
      JobExecutionType jobType,
      AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef,
      Consumer<JobTask> initializer) {
    JobTask task = createTask();
    initializer.accept(task);

    return () -> {
      try {
        return task.call();
      } finally {
        threadPoolManager.releasePermit(jobType);
        if (pollerScheduler != null) {
          pollerScheduler.wakeup();
        }
        cancelTimeoutHandles(handlesRef);
      }
    };
  }

  private JobTask createTask() {
    return new JobTask(
        jobStore,
        resourcePermitService,
        postExecutionHandler,
        nodeIdProvider,
        executionObserver,
        preExecutionValidator,
        beanResolver,
        retryPolicy,
        resilienceStrategy,
        errorSanitizer,
        classPolicy,
        jobLoggerFactory,
        resultPersistenceStrategy,
        authorizationPolicy,
        payloadSerializer,
        effective());
  }

  private JobTimeoutHandler.TimeoutHandles scheduleWatchdog(
      UUID jobId, int timeoutSec, Future<?> fut, Instant executionStartTime) {
    try {
      return timeoutHandler.scheduleTimeoutMonitoring(
          jobId, timeoutSec, fut, executorProvider.getScheduledExecutor(), executionStartTime);
    } catch (Exception e) {
      log.warnf(e, "Watchdog scheduling error for job %s; rejecting execution", jobId);
      throw new RejectedExecutionException("Timeout monitoring could not be scheduled", e);
    }
  }

  private TrackingFutureTask prepareTask(Callable<Void> callable) {
    TrackingFutureTask task = new TrackingFutureTask(callable);
    synchronized (submissionLock) {
      if (!acceptingExecutions.get()) {
        throw new RejectedExecutionException("Job executor is shutting down");
      }
      activeFutures.add(task);
      return task;
    }
  }

  private void submitToExecutor(TrackingFutureTask task) {
    if (task.isCancelled()) {
      throw new RejectedExecutionException("Job execution was canceled before submission");
    }
    ExecutorService executor = executorProvider.getJobExecutor();
    try {
      executor.execute(task);
      if (task.isCancelled()) {
        throw new RejectedExecutionException("Job execution was canceled during submission");
      }
    } catch (RejectedExecutionException e) {
      activeFutures.remove(task);
      throw e;
    }
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }

  private final class TrackingFutureTask extends FutureTask<Void> {

    private final AtomicBoolean runnerStarted = new AtomicBoolean(false);
    private final CountDownLatch runnerExited = new CountDownLatch(1);

    private TrackingFutureTask(Callable<Void> callable) {
      super(callable);
    }

    @Override
    public void run() {
      runnerStarted.set(true);
      try {
        super.run();
      } finally {
        markRunnerExited();
      }
    }

    @Override
    protected void done() {
      if (!runnerStarted.get()) {
        markRunnerExited();
      }
    }

    private boolean awaitRunnerExit(long timeoutNanos) throws InterruptedException {
      return runnerExited.await(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private void markRunnerExited() {
      activeFutures.remove(this);
      runnerExited.countDown();
    }
  }
}
