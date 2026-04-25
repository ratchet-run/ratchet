package run.ratchet.ri.core;

import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.JobLoggerFactory;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Set;
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
import org.jboss.logging.Logger;

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
  private final PollerScheduler pollerScheduler;
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
    this.pollerScheduler = null;
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
      ResultPersistenceStrategy resultPersistenceStrategy) {
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
  }

  private static void cancelTimeoutHandles(
      AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef) {
    JobTimeoutHandler.TimeoutHandles handles = handlesRef.get();
    if (handles != null) {
      handles.cancel();
    }
  }

  public ExecutionResult execute(JobEntity job) {
    JobExecutionType jobType = job.getJobType();
    AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef = new AtomicReference<>();
    Callable<Void> callable = createPermitAwareRunner(job, jobType, handlesRef);
    Instant executionStartTime = Instant.now();

    try {
      Future<Void> future = submitToExecutor(callable);
      handlesRef.set(
          scheduleWatchdog(job.getId(), job.getTimeoutSec(), future, executionStartTime));
      return ExecutionResult.success(future);
    } catch (RejectedExecutionException e) {
      return ExecutionResult.rejected(e);
    }
  }

  public ExecutionResult execute(JobClaimDto claim) {
    JobExecutionType jobType = claim.jobType();
    AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef = new AtomicReference<>();
    Callable<Void> callable = createPermitAwareRunner(claim, jobType, handlesRef);
    Instant executionStartTime = Instant.now();

    try {
      Future<Void> future = submitToExecutor(callable);
      handlesRef.set(scheduleWatchdog(claim.id(), claim.timeoutSec(), future, executionStartTime));
      return ExecutionResult.success(future);
    } catch (RejectedExecutionException e) {
      return ExecutionResult.rejected(e);
    }
  }

  private Callable<Void> createPermitAwareRunner(
      JobEntity job,
      JobExecutionType jobType,
      AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef) {
    JobTask task = createTask();
    task.init(job);

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

  private Callable<Void> createPermitAwareRunner(
      JobClaimDto claim,
      JobExecutionType jobType,
      AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef) {
    JobTask task = createTask();
    task.initFromClaim(claim);

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
        resultPersistenceStrategy);
  }

  private JobTimeoutHandler.TimeoutHandles scheduleWatchdog(
      Long jobId, int timeoutSec, Future<?> fut, Instant executionStartTime) {
    try {
      return timeoutHandler.scheduleTimeoutMonitoring(
          jobId, timeoutSec, fut, executorProvider.getScheduledExecutor(), executionStartTime);
    } catch (Exception e) {
      log.warnf(
          "Watchdog scheduling error for job %s — no timeout monitoring: %s",
          jobId, e.getMessage());
      return null;
    }
  }

  private Future<Void> submitToExecutor(Callable<Void> callable) {
    ExecutorService executor = executorProvider.getJobExecutor();
    TrackingFutureTask task = new TrackingFutureTask(callable);
    activeFutures.add(task);
    try {
      executor.execute(task);
      return task;
    } catch (RejectedExecutionException e) {
      activeFutures.remove(task);
      throw e;
    }
  }

  public int shutdownActiveExecutions() {
    for (TrackingFutureTask task : activeFutures) {
      task.cancel(true);
    }

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!activeFutures.isEmpty() && System.nanoTime() < deadlineNanos) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        break;
      }
      try {
        activeFutures.iterator().next().awaitRunnerExit(Math.min(remainingNanos, 10_000_000L));
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

    private void awaitRunnerExit(long timeoutNanos) throws InterruptedException {
      runnerExited.await(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private void markRunnerExited() {
      activeFutures.remove(this);
      runnerExited.countDown();
    }
  }
}
