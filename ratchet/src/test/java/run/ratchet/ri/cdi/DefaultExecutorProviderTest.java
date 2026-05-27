package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import javax.naming.InitialContext;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;

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

  @Test
  void resolvesConfiguredExecutorJndiNamesFromOptions() throws Exception {
    String jobName = "java:app/concurrent/RatchetVirtualExecutor";
    String scheduledName = "java:app/concurrent/RatchetVirtualScheduledExecutor";
    RatchetOptions options =
        RatchetOptions.builder()
            .execution(
                execution ->
                    execution.jobExecutorJndi(jobName).scheduledExecutorJndi(scheduledName))
            .build();
    InitialContext context = mock(InitialContext.class);
    ExecutorService jobExecutor = mock(ExecutorService.class);
    ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
    when(context.lookup(jobName)).thenReturn(jobExecutor);
    when(context.lookup(scheduledName)).thenReturn(scheduledExecutor);

    DefaultExecutorProvider provider = new TestExecutorProvider(context, options);

    provider.init();

    verify(context).lookup(jobName);
    verify(context).lookup(scheduledName);
    assertSame(jobExecutor, provider.getJobExecutor());
    assertSame(scheduledExecutor, provider.getScheduledExecutor());
  }

  private static final class TestExecutorProvider extends DefaultExecutorProvider {
    private final InitialContext context;

    private TestExecutorProvider(InitialContext context) {
      this.context = context;
    }

    private TestExecutorProvider(InitialContext context, RatchetOptions options) {
      super(options);
      this.context = context;
    }

    @Override
    InitialContext newInitialContext() {
      return context;
    }
  }
}
