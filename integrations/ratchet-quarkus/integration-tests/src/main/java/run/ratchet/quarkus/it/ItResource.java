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

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;

/**
 * Drives the demo app over HTTP so {@link RatchetQuarkusSmokeTest} ({@code @QuarkusTest}) can
 * exercise the extension against a real Quarkus boot. The one-off job is submitted from
 * <em>inside</em> the application process, exactly as an application would.
 */
@Path("/jobs")
public class ItResource {

  @Inject JobSchedulerService scheduler;
  @Inject ItJobs jobs;
  @Inject RatchetOptions options;
  @Inject UnmanagedSubmitter unmanagedSubmitter;

  @POST
  @Path("/submit")
  @Produces(MediaType.TEXT_PLAIN)
  public String submit() {
    scheduler.enqueueNow(jobs::recordRun);
    return "submitted";
  }

  @POST
  @Path("/submit-value")
  @Produces(MediaType.TEXT_PLAIN)
  public String submitValue() {
    String captured = valueToCapture();
    scheduler.enqueueNow(() -> jobs.recordValue(captured));
    return "submitted";
  }

  @POST
  @Path("/submit-arg")
  @Produces(MediaType.TEXT_PLAIN)
  public String submitArg() {
    DemoArg captured = argToCapture();
    scheduler.enqueueNow(() -> jobs.recordArg(captured));
    return "submitted";
  }

  /**
   * Submits from a class the auto-detection heuristic cannot see; see {@link UnmanagedSubmitter}.
   */
  @POST
  @Path("/submit-unmanaged")
  @Produces(MediaType.TEXT_PLAIN)
  public String submitUnmanaged() {
    unmanagedSubmitter.submit(jobs);
    return "submitted";
  }

  @POST
  @Path("/reject-class-policy")
  @Produces(MediaType.TEXT_PLAIN)
  public String rejectClassPolicy() {
    try {
      scheduler.enqueueNow(System::gc);
      return "accepted";
    } catch (SecurityException expected) {
      return "rejected";
    } catch (IllegalArgumentException expected) {
      if (isPayloadValidationRejection(expected)) {
        return "rejected";
      }
      throw expected;
    }
  }

  private static boolean isPayloadValidationRejection(IllegalArgumentException exception) {
    String message = exception.getMessage();
    return message != null && message.startsWith("Job payload validation failed:");
  }

  @GET
  @Path("/executed")
  @Produces(MediaType.TEXT_PLAIN)
  public String executed() {
    return Boolean.toString(jobs.hasExecuted());
  }

  @GET
  @Path("/executed-value")
  @Produces(MediaType.TEXT_PLAIN)
  public String executedValue() {
    return jobs.executedValue();
  }

  @GET
  @Path("/executed-arg")
  @Produces(MediaType.TEXT_PLAIN)
  public String executedArg() {
    return jobs.executedArg();
  }

  @GET
  @Path("/executed-unmanaged")
  @Produces(MediaType.TEXT_PLAIN)
  public String executedUnmanaged() {
    return jobs.executedUnmanaged();
  }

  @GET
  @Path("/recurring-executed")
  @Produces(MediaType.TEXT_PLAIN)
  public String recurringExecuted() {
    return Boolean.toString(jobs.hasRecurringExecuted());
  }

  @GET
  @Path("/auto-migrate-enabled")
  @Produces(MediaType.TEXT_PLAIN)
  public String autoMigrateEnabled() {
    return Boolean.toString(options.schema().autoMigrate());
  }

  // Resolve captures through method calls so javac cannot fold literals into the lambda bodies.
  private static String valueToCapture() {
    return "hello-native";
  }

  private static DemoArg argToCapture() {
    return new DemoArg("demo", 7);
  }
}
