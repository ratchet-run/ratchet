package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import javax.naming.InitialContext;
import org.junit.jupiter.api.Test;

class DefaultExecutorProviderTest {

  @Test
  void initResolvesManagedExecutorsBeforeFirstGetter() throws Exception {
    InitialContext context = mock(InitialContext.class);
    ExecutorService jobExecutor = mock(ExecutorService.class);
    ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
    when(context.lookup(DefaultExecutorProvider.JOB_EXECUTOR_JNDI)).thenReturn(jobExecutor);
    when(context.lookup(DefaultExecutorProvider.SCHEDULED_EXECUTOR_JNDI))
        .thenReturn(scheduledExecutor);

    DefaultExecutorProvider provider = new TestExecutorProvider(context);

    provider.init();

    verify(context).lookup(DefaultExecutorProvider.JOB_EXECUTOR_JNDI);
    verify(context).lookup(DefaultExecutorProvider.SCHEDULED_EXECUTOR_JNDI);
    assertSame(jobExecutor, provider.getJobExecutor());
    assertSame(scheduledExecutor, provider.getScheduledExecutor());
  }

  private static final class TestExecutorProvider extends DefaultExecutorProvider {
    private final InitialContext context;

    private TestExecutorProvider(InitialContext context) {
      this.context = context;
    }

    @Override
    InitialContext newInitialContext() {
      return context;
    }
  }
}
