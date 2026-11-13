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
package run.ratchet.testsuite.idempotency;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.time.Duration;
import java.util.concurrent.Callable;
import javax.naming.InitialContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.exception.DuplicateBusinessKeyException;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;

@ApplicationScoped
public class CallerTransactionBean {

  /** A caller step that may throw a checked exception. */
  @FunctionalInterface
  public interface Step {
    void run() throws Exception;
  }

  /**
   * What the caller saw inside its transaction; statusAfterCatch is a jakarta.transaction.Status
   * value.
   */
  public record CallerResult(
      JobHandle ownWrite, DuplicateBusinessKeyException duplicate, int statusAfterCatch) {}

  @Inject TestJobService jobService;

  /**
   * Runs as the caller's transaction. Writes the caller's own key-less job, runs beforeDuplicate,
   * then submits the duplicate and catches DuplicateBusinessKeyException. Returning normally lets
   * the interceptor commit, so a commit failure reaches the test as an exception.
   */
  @Transactional
  public CallerResult writeOwnJobAndCatchDuplicate(
      Step beforeDuplicate, Callable<JobHandle> duplicate) throws Exception {
    JobHandle ownWrite = jobService.schedule(Duration.ofHours(1), SimpleJob::execute).submit();
    beforeDuplicate.run();
    try {
      JobHandle unexpected = duplicate.call();
      throw new AssertionError(
          "Expected DuplicateBusinessKeyException, got job " + unexpected.id());
    } catch (DuplicateBusinessKeyException e) {
      TransactionSynchronizationRegistry registry =
          InitialContext.doLookup("java:comp/TransactionSynchronizationRegistry");
      return new CallerResult(ownWrite, e, registry.getTransactionStatus());
    }
  }
}
