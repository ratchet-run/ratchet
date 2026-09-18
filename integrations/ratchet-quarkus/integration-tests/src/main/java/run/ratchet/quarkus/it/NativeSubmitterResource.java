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
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import run.ratchet.api.JobSchedulerService;

/** Exercises the lexical lambda owner independently of the scheduler injection site. */
@Path("/native-submitters")
@Produces(MediaType.TEXT_PLAIN)
public class NativeSubmitterResource {
  @Inject Instance<JobSchedulerService> instances;
  @Inject Provider<JobSchedulerService> provider;
  @Inject ItJobs jobs;
  @Inject InheritedSubmitter inherited;
  @Inject ConstructorSubmitter constructor;

  @POST
  @Path("/{kind}")
  public String submit(@PathParam("kind") String kind) {
    ItJobs target = jobs;
    switch (kind) {
      case "instance" -> instances.get().enqueueNow(() -> target.recordSubmitterCase(kind));
      case "provider" -> provider.get().enqueueNow(() -> target.recordSubmitterCase(kind));
      case "constructor" -> constructor.submit(kind);
      case "inherited" -> inherited.submit(kind);
      case "nested" -> new NestedSubmitter().submit(target, kind);
      case "local" -> {
        class LocalSubmitter {
          void submit() {
            JobSchedulerService scheduler = lookup();
            scheduler.enqueueNow(() -> target.recordSubmitterCase(kind));
          }
        }
        new LocalSubmitter().submit();
      }
      case "anonymous" ->
          new Runnable() {
            @Override
            public void run() {
              JobSchedulerService scheduler = lookup();
              scheduler.enqueueNow(() -> target.recordSubmitterCase(kind));
            }
          }.run();
      case "lookup-reference" -> new ReferenceSubmitter().submit(target);
      default -> throw new IllegalArgumentException(kind);
    }
    return "submitted";
  }

  @GET
  @Path("/{kind}")
  public boolean executed(@PathParam("kind") String kind) {
    return jobs.hasSubmitterCase(kind);
  }

  private static JobSchedulerService lookup() {
    return CDI.current().select(JobSchedulerService.class).get();
  }

  static class NestedSubmitter {
    void submit(ItJobs target, String value) {
      JobSchedulerService scheduler = lookup();
      scheduler.enqueueNow(() -> target.recordSubmitterCase(value));
    }
  }

  static class ReferenceSubmitter {
    void submit(ItJobs target) {
      JobSchedulerService scheduler = lookup();
      scheduler.enqueueNow(target::recordLookupReference);
    }
  }

  static class SubmitterBase {
    @Inject JobSchedulerService scheduler;
  }

  @ApplicationScoped
  public static class InheritedSubmitter extends SubmitterBase {
    @Inject ItJobs jobs;

    public void submit(String value) {
      ItJobs target = jobs;
      scheduler.enqueueNow(() -> target.recordSubmitterCase(value));
    }
  }

  @ApplicationScoped
  public static class ConstructorSubmitter {
    private final Provider<JobSchedulerService> scheduler;
    private final ItJobs jobs;

    @Inject
    public ConstructorSubmitter(Provider<JobSchedulerService> scheduler, ItJobs jobs) {
      this.scheduler = scheduler;
      this.jobs = jobs;
    }

    public void submit(String value) {
      ItJobs target = jobs;
      scheduler.get().enqueueNow(() -> target.recordSubmitterCase(value));
    }
  }
}
