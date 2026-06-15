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
package run.ratchet.loadtest.api;

import java.time.Instant;

public class RunStartedResponse {

  public String runId;
  public String workload;
  public int expectedJobs;
  public Instant startedAt;

  public RunStartedResponse() {}

  public RunStartedResponse(String runId, String workload, int expectedJobs, Instant startedAt) {
    this.runId = runId;
    this.workload = workload;
    this.expectedJobs = expectedJobs;
    this.startedAt = startedAt;
  }
}
