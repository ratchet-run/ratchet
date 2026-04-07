package run.ratchet.ri.core;

import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Handles the actual execution of jobs, supporting both virtual and platform threads.
 *
 * <p>This service is responsible for:
 *
 * <ul>
 *   <li>Creating and initializing {@link JobTask} instances
 *   <li>Wrapping runners with permit-release logic
 *   <li>Submitting jobs to virtual threads or platform thread executors
 *   <li>Scheduling timeout watchdogs for running jobs
 * </ul>
 *
 * <p>The caller must have acquired a permit from {@link ThreadPoolManager} before calling {@link
 * #execute(JobEntity)}. The permit will be released automatically when the job completes.
 */
@ApplicationScoped
public class JobExecutorService {

  private static final Logger log = Logger.getLogger(JobExecutorService.class.getName());

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

  // Required by CDI proxy
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
      ClassPolicy classPolicy) {
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
  }

  /**
   * Executes the given job.
   *
   * <p>A permit must have been acquired before calling this method. The permit will be released
   * automatically when the job completes.
   *
   * @param job the job to execute
   * @return the execution result indicating success or rejection
   */
  public ExecutionResult execute(JobEntity job) {
    JobExecutionType jobType = job.getJobType();
    Callable<Void> callable = createPermitAwareRunner(job, jobType);
    Instant executionStartTime = Instant.now();

    try {
      Future<Void> future = submitToExecutor(job.getId(), jobType, callable);
      scheduleWatchdog(job.getId(), job.getTimeoutSec(), future, executionStartTime);
      return ExecutionResult.success(future);
    } catch (RejectedExecutionException e) {
      return ExecutionResult.rejected(e);
    }
  }

  /**
   * Executes a job using only the lightweight claim DTO.
   *
   * @param claim the job claim DTO to execute
   * @return the execution result indicating success or rejection
   */
  public ExecutionResult execute(JobClaimDto claim) {
    JobExecutionType jobType = claim.jobType();
    Callable<Void> callable = createPermitAwareRunner(claim, jobType);
    Instant executionStartTime = Instant.now();

    try {
      Future<Void> future = submitToExecutor(claim.id(), jobType, callable);
      scheduleWatchdog(claim.id(), claim.timeoutSec(), future, executionStartTime);
      return ExecutionResult.success(future);
    } catch (RejectedExecutionException e) {
      return ExecutionResult.rejected(e);
    }
  }

  private Callable<Void> createPermitAwareRunner(JobEntity job, JobExecutionType jobType) {
    JobTask task = createTask();
    task.init(job);

    return () -> {
      try {
        return task.call();
      } finally {
        threadPoolManager.releasePermit(jobType);
      }
    };
  }

  private Callable<Void> createPermitAwareRunner(JobClaimDto claim, JobExecutionType jobType) {
    JobTask task = createTask();
    task.initFromClaim(claim);

    return () -> {
      try {
        return task.call();
      } finally {
        threadPoolManager.releasePermit(jobType);
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
        classPolicy);
  }

  private void scheduleWatchdog(
      Long jobId, int timeoutSec, Future<?> fut, Instant executionStartTime) {
    try {
      timeoutHandler.scheduleTimeoutMonitoring(
          jobId, timeoutSec, fut, executorProvider.getScheduledExecutor(), executionStartTime);
    } catch (Exception e) {
      log.warning(
          "Failed to schedule watchdog for job "
              + jobId
              + " - running without timeout monitoring: "
              + e.getMessage());
    }
  }

  private Future<Void> submitToExecutor(
      Long jobId, JobExecutionType jobType, Callable<Void> callable) {
    if (threadPoolManager.isUseVirtualThreads()) {
      return submitVirtualThread(jobId, jobType, callable);
    }
    ExecutorService executor = executorProvider.getJobExecutor();
    return executor.submit(callable);
  }

  private Future<Void> submitVirtualThread(
      Long jobId, JobExecutionType jobType, Callable<Void> callable) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Thread thread =
        new Thread(
            () -> {
              try {
                callable.call();
                future.complete(null);
              } catch (Exception e) {
                future.completeExceptionally(e);
              }
            },
            "scheduler-vt-" + jobType + "-" + jobId);
    thread.setDaemon(true);
    thread.start();
    return new InterruptibleVirtualThreadFuture(future, thread);
  }

  /**
   * A Future implementation for dedicated threads that supports interruption on cancel.
   *
   * <p>Standard {@link CompletableFuture} does not interrupt the thread on cancel. This wrapper
   * holds a reference to the thread and interrupts it when cancel is called with {@code
   * mayInterruptIfRunning=true}.
   *
   * <p>When virtual threads become available (Java 21+), the thread creation in {@link
   * #submitVirtualThread} can be switched to {@code Thread.ofVirtual()}.
   */
  private record InterruptibleVirtualThreadFuture(CompletableFuture<Void> delegate, Thread thread)
      implements Future<Void> {

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (mayInterruptIfRunning && thread.isAlive()) {
        thread.interrupt();
      }
      return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
      return delegate.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      return delegate.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return delegate.get(timeout, unit);
    }
  }
}
