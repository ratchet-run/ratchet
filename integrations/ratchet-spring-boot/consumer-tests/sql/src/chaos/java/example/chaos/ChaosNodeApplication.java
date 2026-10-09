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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
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
import org.springframework.util.ClassUtils;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.consumer.chaos.DbClock;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;

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
      jdbc.execute(
          """
          create table if not exists chaos_gate_arm (
            point varchar(32), job_id varchar(36), remaining integer,
            primary key (point, job_id))
          """);
      jdbc.execute(
          """
          create table if not exists chaos_gate_hit (
            hit_id varchar(36) primary key, point varchar(32), job_id varchar(36),
            incarnation varchar(36), reached_us bigint, released integer default 0)
          """);
      System.out.println("RATCHET_RUNTIME_READY " + env.getRequiredProperty("local.server.port"));
    };
  }

  @Bean
  Worker chaosWorker(JdbcTemplate jdbc, PlatformTransactionManager manager, Environment env) {
    return new Worker(jdbc, manager, env);
  }

  @Bean
  static BeanPostProcessor chaosStoreGates(ObjectProvider<Worker> worker) {
    // Unordered post-processors run after Ordered processors, including Spring's transactional
    // auto-proxy creator. Resolve the worker only on invocation to avoid early bean creation.
    return new BeanPostProcessor() {
      @Override
      public Object postProcessAfterInitialization(Object bean, String name) {
        if (!(bean instanceof JobStore)) return bean;
        return Proxy.newProxyInstance(
            bean.getClass().getClassLoader(),
            ClassUtils.getAllInterfaces(bean),
            (proxy, method, args) -> {
              if (method.getName().equals("commitCompletion")) {
                worker.getObject().gate("before-commit", ((JobCompletionPlan) args[0]).jobId());
              }
              Object result;
              try {
                result = method.invoke(bean, args);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }
              if (method.getName().equals("claimNextBatch")
                  || method.getName().equals("claimNextBatchOptimized")) {
                for (Object claimed : (List<?>) result) {
                  UUID id =
                      claimed instanceof JobClaimDto dto ? dto.id() : ((JobEntity) claimed).getId();
                  worker.getObject().gate("after-claim", id);
                }
              }
              return result;
            });
      }
    };
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
        @RequestParam(name = "businessKey", defaultValue = "") String businessKey,
        @RequestParam(name = "failTimes", defaultValue = "0") int failTimes,
        @RequestParam(name = "maxRetries", required = false) Integer maxRetries,
        @RequestParam(name = "gatePoint", defaultValue = "") String gatePoint,
        @RequestParam(name = "gateTimes", defaultValue = "1") int gateTimes) {
      return worker.transaction.execute(
          tx -> {
            String submissionTag = "submit:" + UUID.randomUUID();
            var builder =
                scheduler
                    .enqueue(() -> worker.work(run, key, durationMs, failTimes))
                    .withTags("chaos", "run:" + run, submissionTag);
            if (!idempotencyKey.isEmpty()) builder.withIdempotencyKey(idempotencyKey);
            if (!businessKey.isEmpty()) builder.withBusinessKey(businessKey);
            if (maxRetries != null) builder.withMaxRetries(maxRetries);
            // Keep retries inside the settlement window; the default exponential backoff does not.
            builder.withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(200));
            String id = builder.submit().id().toString();
            // Ratchet may return the original handle; only the winning submission writes this tag.
            if (!idempotencyKey.isEmpty()
                && jdbc.queryForObject(
                        "select count(*) from scheduler_job_tag where tag = ?",
                        Integer.class,
                        submissionTag)
                    == 0) throw new DuplicateIdempotencyKeyException(idempotencyKey, null);
            // Publish the arm in its own transaction before the submitted job becomes claimable.
            if (!gatePoint.isEmpty())
              worker.transaction.executeWithoutResult(
                  gateTx ->
                      jdbc.update(
                          "insert into chaos_gate_arm values (?, ?, ?)", gatePoint, id, gateTimes));
            return id;
          });
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

    public void work(String runId, String key, long durationMs, int failTimes)
        throws InterruptedException {
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
        gate("body", JobContext.current().jobId());
        Thread.sleep(durationMs);
      } catch (InterruptedException interrupted) {
        finish(attempt, "interrupted");
        throw interrupted;
      }
      int failures =
          transaction.execute(
              tx ->
                  jdbc.queryForObject(
                      "select count(*) from chaos_attempt where run_id = ? and job_key = ? and"
                          + " outcome = 'failed'",
                      Integer.class,
                      runId,
                      key));
      if (failures < failTimes) {
        finish(attempt, "failed");
        throw new IllegalStateException("Chaos application failure " + (failures + 1));
      }
      finish(attempt, "ok");
    }

    void gate(String point, UUID jobId) throws InterruptedException {
      String hit = UUID.randomUUID().toString();
      boolean armed =
          transaction.execute(
              tx -> {
                int consumed =
                    jdbc.update(
                        """
                        update chaos_gate_arm set remaining = remaining - 1
                        where point = ? and job_id = ? and remaining > 0
                        """,
                        point,
                        jobId.toString());
                if (consumed != 1) return false;
                jdbc.update(
                    "insert into chaos_gate_hit values (?, ?, ?, ?, " + clock + ", 0)",
                    hit,
                    point,
                    jobId.toString(),
                    incarnation);
                return true;
              });
      if (!armed) return;
      long deadline = System.nanoTime() + Duration.ofSeconds(150).toNanos();
      while (System.nanoTime() < deadline) {
        int released =
            transaction.execute(
                tx ->
                    jdbc.queryForObject(
                        "select released from chaos_gate_hit where hit_id = ?",
                        Integer.class,
                        hit));
        if (released != 0) return;
        Thread.sleep(50);
      }
      throw new IllegalStateException("Chaos gate timed out: " + point + " " + jobId + " " + hit);
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
