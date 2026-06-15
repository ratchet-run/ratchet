/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.testsuite.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Exercises Ratchet's virtual-thread execution path on Jakarta EE 11 containers (WildFly 40,
 * GlassFish 8). Compiled and run only under the EE 11 server profiles (the {@code
 * src/test/java-ee11} source root); the EE 10 profiles never see it, so it is not "skipped" there.
 *
 * <p>{@link VirtualThreadOptionsProducer} points the executor at {@link VirtualThreadTestExecutor}
 * (an application-declared {@code @ManagedExecutorDefinition(virtual = true)}) and enables the
 * virtual-thread backpressure model, so the assertions exercise the real end-to-end path.
 *
 * <p><b>Wiring vs. true virtual threads.</b> That jobs run and complete at all proves Ratchet
 * resolved and used the configured executor: {@code DefaultExecutorProvider} fails on first use if
 * {@code java:app/concurrent/RatchetTestVirtualExecutor} cannot be resolved. Whether those threads are
 * <em>virtual</em> depends on the container: GlassFish 8 (the EE 11 RI, via {@code concurro}) honors
 * {@code virtual = true}; WildFly 40.0.0.Final does not yet implement virtual threads for managed
 * executors (its {@code wildfly-concurrency-impl} has no virtual wiring), so it runs the same jobs
 * on platform threads. The hard {@code isVirtual()} assertion therefore runs on GlassFish; WildFly
 * gets the wiring + concurrency check.
 */
class VirtualThreadExecutionIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-ee11-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            VirtualThreadProbeJob.class,
            VirtualThreadOptionsProducer.class,
            VirtualThreadTestExecutor.class,
            TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  /**
   * True only on containers that actually implement virtual threads for Jakarta Concurrency
   * managed executors. GlassFish 8 (the EE 11 reference implementation) does; WildFly 40.0.0.Final
   * binds and uses the {@code virtual = true} executor but still hands out platform threads
   * (verified by running this same deployment on both: {@code isVirtual()} is true on GlassFish,
   * false on WildFly). When WildFly implements it, drop this guard for WildFly.
   */
  private static boolean expectsVirtualThreads() {
    return System.getProperty("testsuite.profile", "").contains("glassfish");
  }

  @BeforeEach
  void resetProbe() {
    VirtualThreadProbeJob.reset();
  }

  @Test
  void job_runsOnConfiguredVirtualExecutor() {
    JobHandle handle = jobService.enqueueNow(VirtualThreadProbeJob::execute);

    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertEquals(1, VirtualThreadProbeJob.invocations());
    if (expectsVirtualThreads()) {
      assertTrue(
          VirtualThreadProbeJob.allRanOnVirtualThreads(),
          "job body must run on a virtual thread; observed threads="
              + VirtualThreadProbeJob.threadNames());
    }
  }

  @Test
  void manyJobs_runConcurrentlyOnConfiguredVirtualExecutor() {
    int jobCount = 20;
    List<JobHandle> handles = new ArrayList<>(jobCount);
    for (int i = 0; i < jobCount; i++) {
      handles.add(jobService.enqueueNow(VirtualThreadProbeJob::execute));
    }

    for (JobHandle handle : handles) {
      JobAssertions.assertJobCompleted(jobCrudStore, handle);
    }

    assertEquals(jobCount, VirtualThreadProbeJob.invocations());
    assertTrue(
        VirtualThreadProbeJob.peakConcurrency() >= 2,
        "expected overlapping execution on the managed executor; peak concurrency="
            + VirtualThreadProbeJob.peakConcurrency());
    if (expectsVirtualThreads()) {
      assertTrue(
          VirtualThreadProbeJob.allRanOnVirtualThreads(),
          "every job body must run on a virtual thread; observed threads="
              + VirtualThreadProbeJob.threadNames());
    }
  }
}
