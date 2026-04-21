package run.ratchet.ri.core;

import run.ratchet.api.RatchetOptions;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
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
  private final JobLoggerFactory jobLoggerFactory;
  private final ResultPersistenceStrategy resultPersistenceStrategy;
  private final PollerScheduler pollerScheduler;

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
    this(
        threadPoolManager,
        timeoutHandler,
        executorProvider,
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
        null,
        context -> new JBossLoggingJobLogger(context.jobId(), null),
        new DefaultResultPersistenceStrategy(RatchetOptions.defaults()));
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

  public ExecutionResult execute(JobEntity job) {
    JobExecutionType jobType = job.getJobType();
    AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef = new AtomicReference<>();
    Callable<Void> callable = createPermitAwareRunner(job, jobType, handlesRef);
    Instant executionStartTime = Instant.now();

    try {
      Future<Void> future = submitToExecutor(job.getId(), jobType, callable);
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
      Future<Void> future = submitToExecutor(claim.id(), jobType, callable);
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

  private static void cancelTimeoutHandles(
      AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef) {
    JobTimeoutHandler.TimeoutHandles handles = handlesRef.get();
    if (handles != null) {
      handles.cancel();
    }
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

  private static final boolean VIRTUAL_THREADS_AVAILABLE = Runtime.version().feature() >= 21;

  private Future<Void> submitToExecutor(
      Long jobId, JobExecutionType jobType, Callable<Void> callable) {
    if (threadPoolManager.isUseVirtualThreads()) {
      return submitDedicatedThread(jobId, jobType, callable);
    }
    ExecutorService executor = executorProvider.getJobExecutor();
    return executor.submit(callable);
  }

  private Future<Void> submitDedicatedThread(
      Long jobId, JobExecutionType jobType, Callable<Void> callable) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    String threadName = "scheduler-" + jobType + "-" + jobId;
    Runnable task =
        () -> {
          try {
            callable.call();
            future.complete(null);
          } catch (Exception e) {
            future.completeExceptionally(e);
          }
        };

    Thread thread;
    if (VIRTUAL_THREADS_AVAILABLE) {
      thread = createVirtualThread(task, threadName);
    } else {
      thread = new Thread(task, threadName);
      thread.setDaemon(true);
    }
    thread.start();
    return new InterruptibleThreadFuture(future, thread);
  }

  private static Thread createVirtualThread(Runnable task, String name) {
    try {
      // Thread.ofVirtual().name(name).unstarted(task) — via reflection for Java 17 compilation
      Object builder = Thread.class.getMethod("ofVirtual").invoke(null);
      builder = builder.getClass().getMethod("name", String.class).invoke(builder, name);
      return (Thread)
          builder.getClass().getMethod("unstarted", Runnable.class).invoke(builder, task);
    } catch (Exception e) {
      log.warnf(
          "Virtual thread creation failed, falling back to platform thread: %s", e.getMessage());
      Thread fallback = new Thread(task, name);
      fallback.setDaemon(true);
      return fallback;
    }
  }

  // Wraps CompletableFuture with a thread reference so cancel(true) can interrupt the thread.
  private record InterruptibleThreadFuture(CompletableFuture<Void> delegate, Thread thread)
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
