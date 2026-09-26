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
package example.runtime.sql;

import example.ratchet.ConsumerApplication;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.*;
import run.ratchet.api.*;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.ri.core.DrainController;
import run.ratchet.spi.SchedulerLifecycleHook;

/** HTTP submission and durable barriers used by process-level consumer tests. */
@Configuration(proxyBeanMethods = false)
public class RuntimeWebApplication {
  public static void main(String[] args) {
    SpringApplication.run(
        new Class<?>[] {ConsumerApplication.class, RuntimeWebApplication.class}, args);
  }

  @Bean
  ApplicationRunner runtimeReady(Environment environment, JdbcTemplate jdbc) {
    return args -> {
      jdbc.execute(
          "create table if not exists runtime_effect (id varchar(100) primary key, executions integer not null, state varchar(20) not null)");
      jdbc.execute(
          "create table if not exists runtime_gate (id varchar(100) primary key, released boolean not null)");
      jdbc.execute(
          "create table if not exists runtime_attempt (attempt_id bigserial primary key, business_id varchar(100), job_id uuid, node_id varchar(100), started_at timestamp with time zone default clock_timestamp(), finished_at timestamp with time zone)");
      System.out.println(
          "RATCHET_RUNTIME_READY " + environment.getRequiredProperty("local.server.port"));
    };
  }

  @Bean
  WorkerJobs runtimeWorkerJobs(
      JdbcTemplate jdbc, PlatformTransactionManager manager, Environment env) {
    return new WorkerJobs(jdbc, manager, env.getRequiredProperty("ratchet.node.id"));
  }

  @Bean
  Submission runtimeSubmission(JdbcTemplate jdbc, JobSchedulerService scheduler, WorkerJobs jobs) {
    return new Submission(jdbc, scheduler, jobs);
  }

  @Bean
  SchedulerLifecycleHook runtimeStopHook(JdbcTemplate jdbc) {
    return new SchedulerLifecycleHook() {
      public void beforeStop() {
        System.out.println("RATCHET_RUNTIME_STOPPING");
      }

      public void afterStop() {
        if (jdbc.queryForObject("select 1", Integer.class) != 1)
          throw new AssertionError("datasource closed before workers");
        System.out.println("RATCHET_RUNTIME_STOPPED_DB_AVAILABLE");
      }
    };
  }

  @Bean
  @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
      name = "fixture.recurring",
      havingValue = "true",
      matchIfMissing = true)
  RecurringProbe runtimeRecurringProbe(JdbcTemplate jdbc) {
    return new RecurringProbe(jdbc);
  }

  public static class RecurringProbe {
    private final JdbcTemplate jdbc;

    RecurringProbe(JdbcTemplate jdbc) {
      this.jdbc = jdbc;
    }

    @Recurring(cron = "0 0 0 1 1 ?", id = "runtime-shared-recurring")
    public void scheduled() {
      jdbc.queryForObject("select 1", Integer.class);
    }
  }

  public static class Submission {
    private final JdbcTemplate jdbc;
    private final JobSchedulerService scheduler;
    private final WorkerJobs jobs;

    Submission(JdbcTemplate jdbc, JobSchedulerService scheduler, WorkerJobs jobs) {
      this.jdbc = jdbc;
      this.scheduler = scheduler;
      this.jobs = jobs;
    }

    @Transactional
    public UUID submit(
        String id, String gate, String resource, boolean rollback, long delay, boolean prepared) {
      if (!prepared)
        jdbc.update(
            "insert into runtime_effect values (?, 0, 'submitted') on conflict (id) do nothing",
            id);
      var builder =
          delay == 0
              ? scheduler.enqueue(() -> jobs.run(id, gate))
              : scheduler.schedule(Duration.ofSeconds(delay), () -> jobs.run(id, gate));
      builder.withIdempotencyKey("runtime-" + id).withBusinessKey(id);
      if (!resource.isEmpty()) builder.withResource(resource);
      var handle = builder.submit();
      if (rollback) throw new IllegalStateException("intentional HTTP transaction rollback");
      return handle.id();
    }
  }

  public static class WorkerJobs {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final String node;

    WorkerJobs(JdbcTemplate jdbc, PlatformTransactionManager manager, String node) {
      this.jdbc = jdbc;
      this.transaction = new TransactionTemplate(manager);
      this.node = node;
    }

    public void run(String id, String gate) throws Exception {
      Long attempt =
          jdbc.queryForObject(
              "insert into runtime_attempt (business_id, job_id, node_id) values (?, ?, ?) returning attempt_id",
              Long.class,
              id,
              JobContext.current().jobId(),
              node);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(150);
      while (!gate.isEmpty()
          && !Boolean.TRUE.equals(
              jdbc.queryForObject(
                  "select released from runtime_gate where id = ?", Boolean.class, gate))) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("fixture gate timed out");
        Thread.sleep(50);
      }
      Thread.sleep(200);
      transaction.executeWithoutResult(
          status -> {
            jdbc.update(
                "update runtime_effect set executions = executions + 1, state = 'executed' where id = ? and state <> 'executed'",
                id);
            jdbc.update(
                "update runtime_attempt set finished_at = clock_timestamp() where attempt_id = ?",
                attempt);
          });
    }
  }

  @RestController
  public static class Control {
    private final Submission submission;
    private final JdbcTemplate jdbc;
    private final DrainController drain;
    private final JobSchedulerService scheduler;

    Control(
        Submission submission,
        JdbcTemplate jdbc,
        DrainController drain,
        JobSchedulerService scheduler) {
      this.submission = submission;
      this.jdbc = jdbc;
      this.drain = drain;
      this.scheduler = scheduler;
    }

    @ExceptionHandler(DuplicateIdempotencyKeyException.class)
    public ResponseEntity<String> duplicate() {
      return ResponseEntity.status(409).body("duplicate-idempotency");
    }

    @PostMapping("/submit")
    public String submit(
        @RequestParam(name = "id") String id,
        @RequestParam(name = "gate", defaultValue = "") String gate,
        @RequestParam(name = "resource", defaultValue = "") String resource,
        @RequestParam(name = "rollback", defaultValue = "false") boolean rollback,
        @RequestParam(name = "delay", defaultValue = "0") long delay,
        @RequestParam(name = "prepared", defaultValue = "false") boolean prepared) {
      return submission.submit(id, gate, resource, rollback, delay, prepared).toString();
    }

    @PostMapping("/retry")
    public String retry(@RequestParam(name = "id") UUID id) {
      return Boolean.toString(scheduler.retryJob(id));
    }

    @PostMapping("/drain")
    public String drain(@RequestParam(name = "enabled") boolean enabled) {
      drain.setDraining(enabled);
      return "ok";
    }
  }
}
