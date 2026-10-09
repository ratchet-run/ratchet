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
package example.chaos;

import example.ratchet.ConsumerApplication;
import java.util.UUID;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.consumer.chaos.DbClock;

/** A real scheduler node with a durable execution ledger. */
@Configuration(proxyBeanMethods = false)
public class ChaosNodeApplication {
  public static void main(String[] args) {
    SpringApplication.run(
        new Class<?>[] {ConsumerApplication.class, ChaosNodeApplication.class}, args);
  }

  @Bean
  ApplicationRunner chaosReady(JdbcTemplate jdbc, Environment env) {
    return args -> {
      jdbc.execute(
          """
          create table if not exists chaos_attempt (
            attempt_id varchar(36) primary key, run_id varchar(64), job_key varchar(128),
            job_id varchar(36), node_id varchar(128), incarnation varchar(36),
            started_us bigint, finished_us bigint null, outcome varchar(16) null)
          """);
      jdbc.execute(
          """
          create table if not exists chaos_commit (
            job_id varchar(36), node_id varchar(128), incarnation varchar(36),
            terminal_status varchar(16), committed_us bigint)
          """);
      System.out.println("RATCHET_RUNTIME_READY " + env.getRequiredProperty("local.server.port"));
    };
  }

  @Bean
  Worker chaosWorker(JdbcTemplate jdbc, PlatformTransactionManager manager, Environment env) {
    return new Worker(jdbc, manager, env);
  }

  @RestController
  public static class Control {
    private final JobSchedulerService scheduler;
    private final Worker worker;
    private final JdbcTemplate jdbc;

    Control(JobSchedulerService scheduler, Worker worker, JdbcTemplate jdbc) {
      this.scheduler = scheduler;
      this.worker = worker;
      this.jdbc = jdbc;
    }

    @PostMapping("/submit")
    public String submit(
        @RequestParam(name = "run") String run,
        @RequestParam(name = "key") String key,
        @RequestParam(name = "durationMs") long durationMs,
        @RequestParam(name = "idempotencyKey", defaultValue = "") String idempotencyKey,
        @RequestParam(name = "businessKey", defaultValue = "") String businessKey) {
      String submissionTag = "submit:" + UUID.randomUUID();
      var builder =
          scheduler
              .enqueue(() -> worker.work(run, key, durationMs))
              .withTags("chaos", "run:" + run, submissionTag);
      if (!idempotencyKey.isEmpty()) builder.withIdempotencyKey(idempotencyKey);
      if (!businessKey.isEmpty()) builder.withBusinessKey(businessKey);
      String id = builder.submit().id().toString();
      // Ratchet may return the original handle; only the winning submission writes this tag.
      if (!idempotencyKey.isEmpty()
          && jdbc.queryForObject(
                  "select count(*) from scheduler_job_tag where tag = ?",
                  Integer.class,
                  submissionTag)
              == 0) throw new DuplicateIdempotencyKeyException(idempotencyKey, null);
      return id;
    }

    @ExceptionHandler(DuplicateIdempotencyKeyException.class)
    public ResponseEntity<String> duplicate() {
      return ResponseEntity.status(409).body("duplicate-idempotency");
    }

    @GetMapping("/node")
    public String node() {
      return worker.node;
    }
  }

  public static class Worker {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final String node;
    private final String incarnation;
    private final String clock;

    Worker(JdbcTemplate jdbc, PlatformTransactionManager manager, Environment env) {
      this.jdbc = jdbc;
      transaction = new TransactionTemplate(manager);
      transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
      node = env.getRequiredProperty("ratchet.node.id");
      incarnation = env.getRequiredProperty("chaos.incarnation");
      clock = DbClock.expression(env.getRequiredProperty("chaos.store"));
    }

    public void work(String runId, String key, long durationMs) throws InterruptedException {
      String attempt = UUID.randomUUID().toString();
      transaction.executeWithoutResult(
          status ->
              jdbc.update(
                  "insert into chaos_attempt values (?, ?, ?, ?, ?, ?, " + clock + ", null, null)",
                  attempt,
                  runId,
                  key,
                  JobContext.current().jobId().toString(),
                  node,
                  incarnation));
      try {
        Thread.sleep(durationMs);
      } catch (InterruptedException interrupted) {
        finish(attempt, "interrupted");
        throw interrupted;
      }
      finish(attempt, "ok");
    }

    private void finish(String attempt, String outcome) {
      transaction.executeWithoutResult(
          status ->
              jdbc.update(
                  "update chaos_attempt set finished_us = "
                      + clock
                      + ", outcome = ? where attempt_id = ?",
                  outcome,
                  attempt));
    }

    @EventListener
    public void completed(JobCompletedEvent event) {
      commit(event.getJobId(), "SUCCEEDED");
    }

    @EventListener
    public void failed(JobDlqEvent event) {
      commit(event.getJobId(), "FAILED");
    }

    private void commit(UUID job, String status) {
      transaction.executeWithoutResult(
          tx ->
              jdbc.update(
                  "insert into chaos_commit values (?, ?, ?, ?, " + clock + ")",
                  job.toString(),
                  node,
                  incarnation,
                  status));
    }
  }
}
