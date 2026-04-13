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
import org.jboss.logging.Logger;

/**
 * Executes jobs on virtual or platform threads. A permit must be acquired before calling {@link
 * #execute}; it is released automatically on completion.
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
   * Executes the given job. A permit must have been acquired before calling this method; it is
   * released automatically on completion.
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
      log.warnf(
          "Failed to schedule watchdog for job %s - running without timeout monitoring: %s",
          jobId, e.getMessage());
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

  // Wraps CompletableFuture with a thread reference so cancel(true) can interrupt the thread.
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
