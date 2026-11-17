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
package run.ratchet.testsuite.transaction;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** MongoDB rejects a unit before any application work begins. */
@EnabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
class TransactionalCheckpointMongoIT extends BaseRatchetIT {
  @Inject private TestJobService scheduler;
  @Inject private JobStore store;

  @Deployment
  public static WebArchive createDeployment() {
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(
            System.getProperty("testsuite.profile", "wildfly-managed"), "mongodb")
        .addClasses(TestJobService.class, MongoJob.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void mongoRejectsUnitBeforeWork() {
    MongoJob.ran.set(false);
    MongoJob.failure.set(null);
    var handle = scheduler.enqueue(MongoJob::execute).withMaxRetries(0).submit();
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertEquals(
                    JobStatus.SUCCEEDED, store.findById(handle.id()).orElseThrow().getStatus()));
    assertInstanceOf(UnsupportedOperationException.class, MongoJob.failure.get());
    assertFalse(MongoJob.ran.get());
  }

  public static class MongoJob {
    static final AtomicBoolean ran = new AtomicBoolean();
    static final AtomicReference<Exception> failure = new AtomicReference<>();

    public static void execute() throws Exception {
      try {
        JobContext.current()
            .runInTransaction(
                cp -> {
                  ran.set(true);
                  return "done";
                });
      } catch (UnsupportedOperationException exception) {
        failure.set(exception);
      }
    }
  }
}
