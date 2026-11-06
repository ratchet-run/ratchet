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
package run.ratchet.testsuite.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.ProgressJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** The default ten-second interval cannot delay a report past the terminal transition. */
class ProgressCompletionIT extends BaseRatchetIT {
  @Inject private TestJobService jobs;
  @Inject private JobStore store;
  @Inject private JobQueryService queries;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), dbType)
        .addClasses(ProgressJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void immediateCompletionSavesProgressWithDefaultInterval() {
    JobHandle handle = jobs.enqueue(ProgressJob::immediate).submit();
    JobAssertions.assertJobCompleted(store, handle);
    var detail = queries.getJobDetail(handle.id()).orElseThrow();
    assertEquals(JobStatus.SUCCEEDED, detail.summary().status());
    assertNotNull(detail.progress());
    assertEquals(5L, detail.progress().done());
    assertEquals(5L, detail.progress().total());
  }
}
