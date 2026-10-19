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
package example.ratchet.verification;

import example.ratchet.verification.VerificationJobs.PojoArgument;
import example.ratchet.verification.VerificationJobs.RecordArgument;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;

@Singleton
@Requires(property = "consumer.native-check", value = "true")
public class NativeVerification implements ApplicationEventListener<StartupEvent> {
  private final JobSchedulerService scheduler;
  private final VerificationJobs jobs;
  private final JobQueryService queries;
  private final ApplicationContext context;

  public NativeVerification(
      JobSchedulerService scheduler,
      VerificationJobs jobs,
      JobQueryService queries,
      ApplicationContext context) {
    this.scheduler = scheduler;
    this.jobs = jobs;
    this.queries = queries;
    this.context = context;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    Thread worker =
        new Thread(
            () -> {
              try {
                run();
                context.close();
                System.exit(0);
              } catch (Throwable failure) {
                failure.printStackTrace();
                System.err.println("RATCHET_VERIFICATION_FAILED " + failure);
                System.exit(1);
              }
            },
            "consumer-verification");
    worker.setDaemon(false);
    worker.start();
  }

  private void run() throws Exception {
    VerificationJobs jobs = this.jobs;
    JobSchedulerService scheduler = this.scheduler;
    String scenario = System.getProperty("consumer.scenario", "checks");
    if (!scenario.equals("checks")) {
      if (scenario.equals("restart")) {
        UUID id = UUID.fromString(System.getProperty("consumer.job-id"));
        waitFor(() -> succeeded(id), "recovered original job " + id);
        marker("RECOVERY");
      } else {
        String value = "durable";
        JobHandle handle = scheduler.enqueue(() -> jobs.durable(value)).submit();
        System.out.println("RATCHET_JOB_ID " + handle.id());
        System.out.flush();
        // The test owns termination in the drain scenario and checks the outcome itself; polling
        // here would race the datasource closing during shutdown.
        if (scenario.equals("drain")) while (true) Thread.sleep(1000);
        waitFor(() -> succeeded(handle.id()), "durable job");
        marker("DURABLE");
      }
      return;
    }
    check(scheduler.enqueue(jobs::reference).submit(), "METHOD_REFERENCE");
    String capture = "CAPTURING_LAMBDA";
    check(scheduler.enqueue(() -> jobs.accept(capture)).submit(), capture);
    String staticValue = "STATIC_METHOD";
    check(
        scheduler.enqueue(() -> VerificationJobs.staticTarget(staticValue)).submit(), staticValue);
    check(new Nested().submit(scheduler, jobs), "NESTED_SUBMITTER");
    Submitter anonymous =
        new Submitter() {
          @Override
          public JobHandle submit(JobSchedulerService scheduler, VerificationJobs jobs) {
            String value = "ANONYMOUS_SUBMITTER";
            return scheduler.enqueue(() -> jobs.accept(value)).submit();
          }
        };
    check(anonymous.submit(scheduler, jobs), "ANONYMOUS_SUBMITTER");
    class Local {
      JobHandle submit(JobSchedulerService scheduler, VerificationJobs jobs) {
        String value = "LOCAL_SUBMITTER";
        return scheduler.enqueue(() -> jobs.accept(value)).submit();
      }
    }
    check(new Local().submit(scheduler, jobs), "LOCAL_SUBMITTER");
    check(new Child().submit(scheduler, jobs), "INHERITED_SUBMITTER");
    RecordArgument record = new RecordArgument("RECORD_ARGUMENT");
    check(scheduler.enqueue(() -> jobs.record(record)).submit(), record.value());
    PojoArgument pojo = new PojoArgument("POJO_ARGUMENT");
    check(scheduler.enqueue(() -> jobs.pojo(pojo)).submit(), pojo.getValue());
    // Objects built inside the lambda are constructed by Ratchet at submit time.
    check(
        scheduler.enqueue(() -> jobs.record(new RecordArgument("INLINE_RECORD"))).submit(),
        "INLINE_RECORD");
    check(
        scheduler.enqueue(() -> jobs.pojo(new PojoArgument("INLINE_POJO"))).submit(),
        "INLINE_POJO");
    waitFor(() -> VerificationJobs.TICKS.get() >= 2, "recurring twice");
    marker("RECURRING");
    for (ApplicationChecks checks : context.getBeansOfType(ApplicationChecks.class))
      checks.verify();
    marker("ALL_CHECKS");
  }

  private boolean succeeded(UUID id) {
    return queries
        .getJobDetail(id)
        .map(d -> d.summary().status() == JobStatus.SUCCEEDED)
        .orElse(false);
  }

  private void check(JobHandle handle, String name) throws Exception {
    waitFor(() -> succeeded(handle.id()) && VerificationJobs.EXECUTED.contains(name), name);
    marker(name);
  }

  public static void marker(String name) {
    System.out.println("RATCHET_CHECK " + name);
  }

  public static void waitFor(BooleanSupplier condition, String name) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(100).toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadline) throw new IllegalStateException("Timeout: " + name);
      Thread.sleep(100);
    }
  }

  interface Submitter {
    JobHandle submit(JobSchedulerService scheduler, VerificationJobs jobs);
  }

  public static class Nested implements Submitter {
    public JobHandle submit(JobSchedulerService scheduler, VerificationJobs jobs) {
      String value = "NESTED_SUBMITTER";
      return scheduler.enqueue(() -> jobs.accept(value)).submit();
    }
  }

  public static class Parent implements Submitter {
    public JobHandle submit(JobSchedulerService scheduler, VerificationJobs jobs) {
      String value = "INHERITED_SUBMITTER";
      return scheduler.enqueue(() -> jobs.accept(value)).submit();
    }
  }

  public static class Child extends Parent {}
}
