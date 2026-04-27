package run.ratchet.ri.cdi;

import run.ratchet.spi.ExecutorProvider;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import javax.naming.InitialContext;
import javax.naming.NamingException;

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
 * <p>Lookups are performed lazily on first call and the resolved references are cached for the
 * lifetime of the bean.
 *
 * <p>Users can override by providing their own {@code @Alternative @Priority(APPLICATION)
 * ExecutorProvider} bean — for example {@link StandaloneExecutorProvider} for plain-CDI/SE runs.
 */
@ApplicationScoped
public class DefaultExecutorProvider implements ExecutorProvider {

  static final String JOB_EXECUTOR_JNDI = "java:comp/DefaultManagedExecutorService";
  static final String SCHEDULED_EXECUTOR_JNDI = "java:comp/DefaultManagedScheduledExecutorService";

  private volatile ExecutorService resolvedJobExecutor;
  private volatile ScheduledExecutorService resolvedScheduledExecutor;

  @Override
  public ExecutorService getJobExecutor() {
    ExecutorService executor = resolvedJobExecutor;
    if (executor == null) {
      synchronized (this) {
        executor = resolvedJobExecutor;
        if (executor == null) {
          executor = lookup(JOB_EXECUTOR_JNDI, ExecutorService.class);
          resolvedJobExecutor = executor;
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
          executor = lookup(SCHEDULED_EXECUTOR_JNDI, ScheduledExecutorService.class);
          resolvedScheduledExecutor = executor;
        }
      }
    }
    return executor;
  }

  private static <T> T lookup(String jndiName, Class<T> type) {
    try {
      return type.cast(new InitialContext().lookup(jndiName));
    } catch (NamingException e) {
      throw new IllegalStateException(
          "DefaultExecutorProvider could not resolve "
              + jndiName
              + " from JNDI. This name is required by Jakarta Concurrency 3.0+; if you are running"
              + " outside a Jakarta EE 10+ container, provide an @Alternative ExecutorProvider bean"
              + " (e.g., StandaloneExecutorProvider).",
          e);
    } catch (ClassCastException e) {
      throw new IllegalStateException(
          "DefaultExecutorProvider expected " + jndiName + " to be a " + type.getName(), e);
    }
  }
}
