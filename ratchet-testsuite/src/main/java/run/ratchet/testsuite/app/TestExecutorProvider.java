package run.ratchet.testsuite.app;

import run.ratchet.spi.ExecutorProvider;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Test deployment override for containers that do not wire {@code @Resource(lookup = ...)} fields
 * in CDI library beans.
 *
 * <p>The provider still uses the Jakarta Concurrency default managed executors; it just resolves
 * them directly from their standard JNDI names.
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class TestExecutorProvider implements ExecutorProvider {

  private volatile ExecutorService jobExecutor;
  private volatile ScheduledExecutorService scheduledExecutor;

  @Override
  public ExecutorService getJobExecutor() {
    ExecutorService executor = jobExecutor;
    if (executor == null) {
      synchronized (this) {
        executor = jobExecutor;
        if (executor == null) {
          executor = lookup("java:comp/DefaultManagedExecutorService", ExecutorService.class);
          jobExecutor = executor;
        }
      }
    }
    return executor;
  }

  @Override
  public ScheduledExecutorService getScheduledExecutor() {
    ScheduledExecutorService executor = scheduledExecutor;
    if (executor == null) {
      synchronized (this) {
        executor = scheduledExecutor;
        if (executor == null) {
          executor =
              lookup(
                  "java:comp/DefaultManagedScheduledExecutorService",
                  ScheduledExecutorService.class);
          scheduledExecutor = executor;
        }
      }
    }
    return executor;
  }

  private static <T> T lookup(String name, Class<T> type) {
    try {
      return type.cast(new InitialContext().lookup(name));
    } catch (NamingException e) {
      throw new IllegalStateException("Unable to resolve managed executor: " + name, e);
    }
  }
}
