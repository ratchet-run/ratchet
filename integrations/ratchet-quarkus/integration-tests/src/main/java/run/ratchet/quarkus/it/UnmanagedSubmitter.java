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
package run.ratchet.quarkus.it;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.quarkus.runtime.RegisterJobSubmitter;

/**
 * Submits an inline capturing lambda without ever declaring a {@link JobSchedulerService} field or
 * method parameter, so the extension's auto-detection heuristic cannot see it. {@link
 * RegisterJobSubmitter} is the only reason this class's bytecode reaches the native image; drop the
 * annotation and the submission below fails in native with {@code Bytecode not found}.
 *
 * <p>The scheduler is looked up programmatically on purpose. The heuristic inspects fields and
 * method parameters, and a constructor is a method, so constructor injection would still match it
 * and this class would prove nothing.
 */
@ApplicationScoped
@RegisterJobSubmitter
public class UnmanagedSubmitter {

  public void submit(ItJobs jobs) {
    String captured = valueToCapture();
    JobSchedulerService scheduler = CDI.current().select(JobSchedulerService.class).get();
    scheduler.enqueueNow(() -> jobs.recordUnmanaged(captured));
  }

  // A method call, not a literal, so javac cannot fold the value into the lambda body.
  private static String valueToCapture() {
    return "unmanaged-native";
  }
}
