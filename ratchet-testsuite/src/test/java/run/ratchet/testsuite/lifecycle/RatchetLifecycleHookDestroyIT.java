package run.ratchet.testsuite.lifecycle;

import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.cdi.RatchetLifecycle;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Verifies that {@link RatchetLifecycle} releases each resolved {@link SchedulerLifecycleHook} via
 * {@code Instance.destroy()} on shutdown — required for {@code @Dependent}-scoped hooks to have
 * their {@code @PreDestroy} methods invoked. Exercises the real CDI container (Weld in WildFly).
 *
 * <p>Pre-fix, the {@code lifecycleHooks} field was held as {@code Iterable<SchedulerLifecycleHook>}
 * (cast from {@code Instance<>}), losing the {@code destroy()} capability and leaking dependent
 * instances. Without a real container test, a unit test cannot prove that the {@code destroy} call
 * actually fires {@code @PreDestroy} on a dependent bean.
 *
 * <p>This deployment intentionally contains only one test method: invoking {@code onShutdown()}
 * tears down the Ratchet runtime, so additional tests sharing the deployment would fail.
 */
class RatchetLifecycleHookDestroyIT extends BaseRatchetIT {

  @Inject private RatchetLifecycle lifecycle;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(TrackingDependentHook.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void onShutdown_invokesPreDestroyOnDependentScopedHook() throws Exception {
    // onStartup fired during deployment via @Observes @Initialized(ApplicationScoped); by the time
    // this test runs, the beforeStart/afterStart hooks have already been called on our hook.
    assertTrue(
        TrackingDependentHook.beforeStartCount.get() >= 1,
        "beforeStart must fire during deployment startup");
    assertTrue(
        TrackingDependentHook.afterStartCount.get() >= 1,
        "afterStart must fire during deployment startup");

    // Reflectively invoke package-private onShutdown() — drives lifecycle through beforeStop ->
    // afterStop -> destroyHooks(), which calls Instance.destroy(hook) for each resolved hook.
    Method onShutdown = RatchetLifecycle.class.getDeclaredMethod("onShutdown");
    onShutdown.setAccessible(true);
    onShutdown.invoke(lifecycle);

    assertTrue(
        TrackingDependentHook.beforeStopCount.get() >= 1, "beforeStop must fire during shutdown");
    assertTrue(
        TrackingDependentHook.afterStopCount.get() >= 1, "afterStop must fire during shutdown");
    assertTrue(
        TrackingDependentHook.preDestroyCalled.get(),
        "@PreDestroy must fire on the @Dependent hook when Instance.destroy(hook) is called by "
            + "destroyHooks(). Pre-fix, the field was held as Iterable<>, losing destroy() and "
            + "leaking dependent instances.");
  }

  @Dependent
  public static class TrackingDependentHook implements SchedulerLifecycleHook {

    static final AtomicInteger beforeStartCount = new AtomicInteger();
    static final AtomicInteger afterStartCount = new AtomicInteger();
    static final AtomicInteger beforeStopCount = new AtomicInteger();
    static final AtomicInteger afterStopCount = new AtomicInteger();
    static final AtomicBoolean preDestroyCalled = new AtomicBoolean();

    @Override
    public void beforeStart() {
      beforeStartCount.incrementAndGet();
    }

    @Override
    public void afterStart() {
      afterStartCount.incrementAndGet();
    }

    @Override
    public void beforeStop() {
      beforeStopCount.incrementAndGet();
    }

    @Override
    public void afterStop() {
      afterStopCount.incrementAndGet();
    }

    @PreDestroy
    void onPreDestroy() {
      preDestroyCalled.set(true);
    }
  }
}
