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
package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.asset.ClassLoaderAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.AbstractDelayedSchedulingContract;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.SteppingTestClock;
import run.ratchet.tck.api.TckJobs;
import run.ratchet.tck.util.ConcurrentTestRunner;
import run.ratchet.testsuite.tck.clocked.ClockedTestProducers;
import run.ratchet.testsuite.tck.clocked.InMemoryJobStore;
import run.ratchet.testsuite.tck.clocked.RiClockedTckRuntime;
import run.ratchet.testsuite.tck.clocked.ThrowingJobStoreBase;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * RI subclass of {@link AbstractDelayedSchedulingContract}, run against a {@link
 * RiClockedTckRuntime} backed by an in-memory {@code JobStore} and a {@link SteppingTestClock}. The
 * clocked variant is required because the contract drives both {@code scheduledTime} and the
 * claim-eligibility filter from a single logical clock, which the production MySQL store cannot
 * satisfy without invasive SQL changes.
 *
 * <p>CDI alternatives that wire the in-memory store and the test clock are scoped to this
 * deployment via {@code beans-clocked.xml} (bundled as {@code WEB-INF/beans.xml}). Other Ri*ITs are
 * unaffected.
 */
@ExtendWith(ArquillianExtension.class)
class RiDelayedSchedulingIT extends AbstractDelayedSchedulingContract {

  @Inject private RiClockedTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    WebArchive archive =
        RatchetArchiveBuilder.create()
            .addRatchetDependencies(profile, dbType)
            .addPackage(RatchetTckRuntime.class.getPackage())
            .addPackage(ConcurrentTestRunner.class.getPackage())
            .addClasses(
                ListenerProbe.class,
                TckJobs.class,
                SteppingTestClock.class,
                ThrowingJobStoreBase.class,
                InMemoryJobStore.class,
                RiRatchetTckRuntime.class,
                RiClockedTckRuntime.class,
                ClockedTestProducers.class)
            .addStoreInfrastructure()
            .build();
    archive.addAsWebInfResource(new ClassLoaderAsset("beans-clocked.xml"), "beans.xml");
    return archive;
  }
}
