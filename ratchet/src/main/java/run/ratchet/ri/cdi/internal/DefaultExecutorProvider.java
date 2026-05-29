package run.ratchet.ri.cdi.internal;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.jboss.logging.Logger;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.ExecutorProvider;

/**
 * Default {@link ExecutorProvider} that delegates to the container's managed executor services.
 *
 * <p>Resolves {@code java:comp/DefaultManagedExecutorService} and {@code
 * java:comp/DefaultManagedScheduledExecutorService} via direct JNDI lookup. These names are
 * specified by Jakarta Concurrency 3.0+ and bound by every compliant Jakarta EE 10 (or later)
 * container (verified on WildFly, Payara, OpenLiberty).
 *
 * <p><b>Why JNDI rather than {@code @Resource} injection.</b> Some containers (e.g. Payara) do not
 * honor {@code @Resource(lookup=...)} on CDI library beans — they try to bind {@code
 * java:comp/env/<class>/<field>} and fail deployment when that env-entry isn't present. Direct
 * lookup of the well-known names is portable across all compliant runtimes and returns the same
 * context-propagating {@code ManagedExecutorService} that injection would.
 *
 * <p>Container-managed instances resolve both names during CDI startup, while direct-construction
 * test paths still resolve lazily on first use. Resolved references are cached for the lifetime of
 * the bean.
 *
 * <p>The two JNDI names are configurable via {@code ratchet.worker.job-executor-jndi} and {@code
 * ratchet.worker.scheduled-executor-jndi} (defaulting to the well-known container names above).
 * Pointing {@code job-executor-jndi} at a virtual-thread-backed executor runs jobs on that
 * executor; whether its threads are actually virtual is the container's decision, since {@code
 * virtual = true} is a request a runtime may ignore (Eclipse GlassFish 8 honors it; WildFly 40 does
 * not yet). Jakarta exposes no API to verify this at runtime. On Jakarta EE 11 the application
 * declares one with {@code @ManagedExecutorDefinition(name =
 * "java:app/concurrent/MyVirtualExecutor", virtual = true)} on any {@code @ApplicationScoped} bean
 * and sets {@code job-executor-jndi} to that name. The values flow in through {@link
 * RatchetOptions#execution()}.
 *
 * <p>Users can override by providing their own {@code @Alternative @Priority(APPLICATION)
 * ExecutorProvider} bean — for example {@code StandaloneExecutorProvider} for plain-CDI/SE runs.
 *
 * @apiNote Internal RI implementation. Applications interact with the {@link ExecutorProvider} SPI,
 *     not this class. Public visibility is retained only to support negative-control integration
 *     tests that assert CDI resolves the production bean. Not part of the supported API surface.
 */
@ApplicationScoped
public class DefaultExecutorProvider implements ExecutorProvider {

  private static final Logger log = Logger.getLogger(DefaultExecutorProvider.class);

  static final String JOB_EXECUTOR_JNDI = "java:comp/DefaultManagedExecutorService";
  static final String SCHEDULED_EXECUTOR_JNDI = "java:comp/DefaultManagedScheduledExecutorService";

  private final String jobExecutorJndi;
  private final String scheduledExecutorJndi;
  private final String virtualExecutorJndi;

  private volatile ExecutorService resolvedJobExecutor;
  private volatile ScheduledExecutorService resolvedScheduledExecutor;
  private volatile ExecutorService resolvedVirtualExecutor;

  /** Direct-construction path (tests, plain-SE): uses the well-known container JNDI names. */
  protected DefaultExecutorProvider() {
    this.jobExecutorJndi = JOB_EXECUTOR_JNDI;
    this.scheduledExecutorJndi = SCHEDULED_EXECUTOR_JNDI;
    this.virtualExecutorJndi = null;
  }

  @Inject
  public DefaultExecutorProvider(RatchetOptions options) {
    this.jobExecutorJndi = options.execution().jobExecutorJndi();
    this.scheduledExecutorJndi = options.execution().scheduledExecutorJndi();
    this.virtualExecutorJndi = options.execution().virtualExecutorJndi();
  }

  @PostConstruct
  void init() {
    // Best-effort eager resolution. A deployment-defined executor (e.g. an
    // @ManagedExecutorDefinition or a web.xml <managed-executor>) may not be bound yet when CDI
    // beans initialize on some containers (notably GlassFish, which binds such resources after
    // CDI startup), whereas the well-known java:comp defaults always are. Rather than abort
    // deployment, defer to lazy resolution on first use; a genuinely missing name still surfaces
    // from the getters when the first job runs.
    try {
      InitialContext context = newInitialContext();
      synchronized (this) {
        if (resolvedJobExecutor == null) {
          resolvedJobExecutor = lookup(context, jobExecutorJndi, ExecutorService.class);
        }
        if (resolvedScheduledExecutor == null) {
          resolvedScheduledExecutor =
              lookup(context, scheduledExecutorJndi, ScheduledExecutorService.class);
        }
      }
    } catch (IllegalStateException e) {
      // Only a JNDI-naming failure (the name isn't bound yet) is safe to defer to the lazy
      // getters. lookup()/newInitialContext() wrap NamingException as the cause in that case.
      // Anything else — e.g. a ClassCastException from a wrong executor type — is a real
      // misconfiguration; rethrow it so deployment fails loudly instead of at the first job.
      if (!(e.getCause() instanceof NamingException)) {
        throw e;
      }
      log.debugf(
          e,
          "Eager managed-executor resolution deferred to first use (job=%s, scheduled=%s)",
          jobExecutorJndi,
          scheduledExecutorJndi);
    }
  }

  InitialContext newInitialContext() {
    try {
      return new InitialContext();
    } catch (NamingException e) {
      throw new IllegalStateException(
          "DefaultExecutorProvider could not create an InitialContext. If you are running outside"
              + " a Jakarta EE 10+ container, provide an @Alternative ExecutorProvider bean"
              + " such as StandaloneExecutorProvider.",
          e);
    }
  }

  private <T> T lookup(String jndiName, Class<T> type) {
    return lookup(newInitialContext(), jndiName, type);
  }

  private static <T> T lookup(InitialContext context, String jndiName, Class<T> type) {
    try {
      return type.cast(context.lookup(jndiName));
    } catch (NamingException e) {
      throw new IllegalStateException(
          "DefaultExecutorProvider could not resolve "
              + jndiName
              + " from JNDI. This name is required by Jakarta Concurrency 3.0+; if you are running"
              + " outside a Jakarta EE 10+ container, provide an @Alternative ExecutorProvider bean"
              + " such as StandaloneExecutorProvider.",
          e);
    } catch (ClassCastException e) {
      throw new IllegalStateException(
          "DefaultExecutorProvider expected " + jndiName + " to be a " + type.getName(), e);
    }
  }

  @Override
  public ExecutorService getJobExecutor() {
    ExecutorService executor = resolvedJobExecutor;
    if (executor == null) {
      synchronized (this) {
        executor = resolvedJobExecutor;
        if (executor == null) {
          executor = lookup(jobExecutorJndi, ExecutorService.class);
          resolvedJobExecutor = executor;
        }
      }
    }
    return executor;
  }

  @Override
  public Optional<ExecutorService> getJobExecutor(String target) {
    if (ExecutorTargets.PLATFORM.equals(target)) {
      return Optional.of(getJobExecutor());
    }
    if (ExecutorTargets.VIRTUAL.equals(target) && virtualExecutorJndi != null) {
      return Optional.of(virtualExecutor());
    }
    return Optional.empty();
  }

  private ExecutorService virtualExecutor() {
    ExecutorService executor = resolvedVirtualExecutor;
    if (executor == null) {
      synchronized (this) {
        executor = resolvedVirtualExecutor;
        if (executor == null) {
          // Resolved lazily on first use: a deployment-defined virtual executor may not be bound
          // when CDI beans initialize (notably on GlassFish), so eager resolution is skipped.
          executor = lookup(virtualExecutorJndi, ExecutorService.class);
          resolvedVirtualExecutor = executor;
        }
      }
    }
    return executor;
  }

  @Override
  public ScheduledExecutorService getScheduledExecutor() {
    ScheduledExecutorService executor = resolvedScheduledExecutor;
    if (executor == null) {
      synchronized (this) {
        executor = resolvedScheduledExecutor;
        if (executor == null) {
          executor = lookup(scheduledExecutorJndi, ScheduledExecutorService.class);
          resolvedScheduledExecutor = executor;
        }
      }
    }
    return executor;
  }
}
