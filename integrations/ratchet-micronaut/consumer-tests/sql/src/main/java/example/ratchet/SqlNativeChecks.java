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
package example.ratchet;

import example.ratchet.verification.ApplicationChecks;
import example.ratchet.verification.NativeVerification;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.CircuitBreakerOpenException;

@Singleton
@Requires(property = "consumer.native-check", value = "true")
public class SqlNativeChecks implements ApplicationChecks {
  private final ConsumerRequiresNewSubmission submission;
  private final JobQueryService queries;
  private final NativeProtectedService protectedService;

  public SqlNativeChecks(
      ConsumerRequiresNewSubmission submission,
      JobQueryService queries,
      NativeProtectedService protectedService) {
    this.submission = submission;
    this.queries = queries;
    this.protectedService = protectedService;
  }

  @Override
  public void verify() throws Exception {
    var handle = submission.submit("native-requires-new");
    NativeVerification.waitFor(
        () ->
            queries
                .getJobDetail(handle.id())
                .map(d -> d.summary().status() == JobStatus.SUCCEEDED)
                .orElse(false),
        "REQUIRES_NEW commit");
    NativeVerification.marker("TRANSACTION");
    for (int i = 0; i < 3; i++) {
      try {
        protectedService.fail();
        throw new AssertionError("Failure was swallowed");
      } catch (IllegalStateException expected) {
      }
    }
    try {
      protectedService.fail();
      throw new AssertionError("Circuit did not open");
    } catch (CircuitBreakerOpenException expected) {
    }
    NativeVerification.marker("CIRCUIT_BREAKER");
  }
}
