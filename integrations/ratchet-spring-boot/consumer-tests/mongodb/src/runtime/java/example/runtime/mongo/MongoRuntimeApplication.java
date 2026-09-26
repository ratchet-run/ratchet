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
package example.runtime.mongo;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import example.ratchet.mongo.MongoConsumerApplication;
import java.time.Duration;
import java.util.UUID;
import org.bson.Document;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import run.ratchet.api.*;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.ri.core.DrainController;

@Configuration(proxyBeanMethods = false)
public class MongoRuntimeApplication {
  public static void main(String[] args) {
    SpringApplication.run(
        new Class<?>[] {MongoConsumerApplication.class, MongoRuntimeApplication.class}, args);
  }

  @Bean
  ApplicationRunner runtimeReady(Environment environment) {
    return args ->
        System.out.println(
            "RATCHET_RUNTIME_READY " + environment.getRequiredProperty("local.server.port"));
  }

  @Bean
  Jobs runtimeJobs(MongoDatabaseFactory factory, Environment environment) {
    return new Jobs(factory.getMongoDatabase(), environment.getRequiredProperty("ratchet.node.id"));
  }

  public static class Jobs {
    final MongoDatabase database;
    final String node;

    Jobs(MongoDatabase database, String node) {
      this.database = database;
      this.node = node;
    }

    public void run(String id, String gate) throws Exception {
      String attempt = UUID.randomUUID().toString();
      database
          .getCollection("runtime_attempt")
          .insertOne(
              new Document("_id", attempt)
                  .append("business_id", id)
                  .append("job_id", JobContext.current().jobId().toString())
                  .append("node_id", node));
      long deadline = System.nanoTime() + Duration.ofSeconds(150).toNanos();
      while (!gate.isEmpty()
          && !Boolean.TRUE.equals(
              database
                  .getCollection("runtime_gate")
                  .find(eq("_id", gate))
                  .first()
                  .getBoolean("released"))) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("fixture gate timed out");
        Thread.sleep(50);
      }
      database
          .getCollection("runtime_effect")
          .updateOne(
              and(eq("_id", id), eq("state", "submitted")),
              combine(set("state", "executed"), inc("executions", 1)));
      database
          .getCollection("runtime_attempt")
          .updateOne(eq("_id", attempt), set("finished", true));
    }
  }

  @RestController
  public static class Control {
    private final Jobs jobs;
    private final JobSchedulerService scheduler;
    private final DrainController drain;

    Control(Jobs jobs, JobSchedulerService scheduler, DrainController drain) {
      this.jobs = jobs;
      this.scheduler = scheduler;
      this.drain = drain;
    }

    @ExceptionHandler(DuplicateIdempotencyKeyException.class)
    public ResponseEntity<String> duplicate() {
      return ResponseEntity.status(409).body("duplicate-idempotency");
    }

    @PostMapping("/submit")
    public String submit(
        @RequestParam(name = "id") String id,
        @RequestParam(name = "gate", defaultValue = "") String gate) {
      jobs.database
          .getCollection("runtime_effect")
          .updateOne(
              eq("_id", id),
              combine(setOnInsert("state", "submitted"), setOnInsert("executions", 0)),
              new UpdateOptions().upsert(true));
      return scheduler
          .enqueue(() -> jobs.run(id, gate))
          .withIdempotencyKey("runtime-" + id)
          .withBusinessKey(id)
          .submit()
          .id()
          .toString();
    }

    @PostMapping("/drain")
    public String drain(@RequestParam(name = "enabled") boolean enabled) {
      drain.setDraining(enabled);
      return "ok";
    }

    @PostMapping("/retry")
    public String retry(@RequestParam(name = "id") UUID id) {
      return Boolean.toString(scheduler.retryJob(id));
    }
  }
}
