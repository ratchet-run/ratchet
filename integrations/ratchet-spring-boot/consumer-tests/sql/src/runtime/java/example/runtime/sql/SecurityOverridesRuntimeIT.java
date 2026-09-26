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

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import example.ratchet.ConsumerApplication;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import run.ratchet.api.*;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.ri.cdi.internal.JsonbPayloadSerializer;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.security.PermitAllJobAuthorizationPolicy;
import run.ratchet.spi.*;

public class SecurityOverridesRuntimeIT {
  static final ThreadLocal<String> caller = new ThreadLocal<>();
  static final AtomicReference<String> received = new AtomicReference<>();

  @Test
  void customSecurityAndSerializationBeansControlActualPersistedAndExecutedJobs() {
    received.set(null);
    try (var database = SqlDatabase.start()) {
      var properties = RuntimeSupport.properties(database);
      properties.put("ratchet.security.mask-payloads", true);
      try (var context =
          new SpringApplicationBuilder(ConsumerApplication.class, Overrides.class)
              .properties(properties)
              .run()) {
        var scheduler = context.getBean(JobSchedulerService.class);
        var queries = context.getBean(JobQueryService.class);
        var jdbc = context.getBean(JdbcTemplate.class);
        var policy = context.getBean(OwnerPolicy.class);
        var serializer = context.getBean(WireSerializer.class);
        assertThat(context.getBeansOfType(JobAuthorizationPolicy.class)).hasSize(1);
        assertThat(context.getBeansOfType(PayloadSerializer.class)).hasSize(1);
        caller.set("denied");
        assertThatThrownBy(() -> scheduler.enqueue(() -> accept("forbidden")).submit())
            .isInstanceOf(JobAuthorizationException.class)
            .hasMessage("fixture create denied");
        assertThat(jdbc.queryForObject("select count(*) from scheduler_job", Integer.class))
            .isZero();
        caller.set("alice");
        var handle =
            scheduler
                .enqueue(() -> accept("secret-value"))
                .withParam("privateField", "masked-only-in-read-api")
                .withParam("publicField", "visible")
                .submit();
        caller.remove();
        RuntimeSupport.status(context, handle, JobStatus.SUCCEEDED);
        assertThat(received).hasValue("secret-value");
        assertThat(policy.owner).hasValue("alice");
        assertThat(policy.workerCaller).hasValue("none");
        assertThat(queries.getJobDetail(handle.id()).orElseThrow().summary().callerPrincipal())
            .isEqualTo("alice");
        var params = queries.getJobDetail(handle.id()).orElseThrow().params();
        assertThat(params.get("privateField")).doesNotContain("masked-only-in-read-api");
        assertThat(params.get("publicField")).isEqualTo("visible");
        assertThat(
                jdbc.queryForObject(
                    "select cast(payload as text) from scheduler_job where job_id = ?",
                    String.class,
                    handle.id()))
            .contains("transport-value")
            .doesNotContain("secret-value");
        assertThat(serializer.writes.get()).isPositive();
        assertThat(serializer.reads.get()).isPositive();

        received.set(null);
        context.getBean(DrainController.class).setDraining(true);
        caller.set("revoked");
        var denied = scheduler.enqueue(() -> accept("must-not-run")).withMaxRetries(5).submit();
        caller.remove();
        policy.revoke = true;
        context.getBean(DrainController.class).setDraining(false);
        RuntimeSupport.status(context, denied, JobStatus.FAILED);
        await()
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() -> assertThat(queries.getExecutionHistory(denied.id())).hasSize(1));
        assertThat(queries.getJobDetail(denied.id()).orElseThrow().summary().lastError())
            .contains("fixture execute denied");
        assertThat(received).hasValue(null);
        assertThat(policy.owner).hasValue("revoked");
      } finally {
        caller.remove();
      }
    }
  }

  public static void accept(String value) {
    received.set(value);
  }

  static class OwnerPolicy extends PermitAllJobAuthorizationPolicy {
    final AtomicReference<String> owner = new AtomicReference<>();
    final AtomicReference<String> workerCaller = new AtomicReference<>();
    volatile boolean revoke;

    @Override
    public void checkCreate(UUID id, String principal) {
      if ("denied".equals(principal))
        throw new JobAuthorizationException(id, "create", principal, "fixture create denied");
    }

    @Override
    public void checkExecute(UUID id, String principal) {
      owner.set(principal);
      workerCaller.set(Optional.ofNullable(caller.get()).orElse("none"));
      if (revoke && "revoked".equals(principal))
        throw new JobAuthorizationException(id, "execute", principal, "fixture execute denied");
    }
  }

  static class WireSerializer implements PayloadSerializer {
    final JsonbPayloadSerializer delegate = new JsonbPayloadSerializer();
    final AtomicInteger writes = new AtomicInteger();
    final AtomicInteger reads = new AtomicInteger();

    public String serialize(Object value) {
      writes.incrementAndGet();
      String json = delegate.serialize(value);
      return json == null ? null : json.replace("secret-value", "transport-value");
    }

    public <T> T deserialize(String value, Class<T> type) {
      reads.incrementAndGet();
      return delegate.deserialize(
          value == null ? null : value.replace("transport-value", "secret-value"), type);
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class Overrides {
    @Bean
    PrincipalSource fixturePrincipal() {
      return () -> Optional.ofNullable(caller.get());
    }

    @Bean
    OwnerPolicy fixtureAuthorization() {
      return new OwnerPolicy();
    }

    @Bean
    PayloadMaskingPolicy fixtureMasking() {
      return name -> name.equals("privateField");
    }

    @Bean
    WireSerializer fixtureSerializer() {
      return new WireSerializer();
    }
  }
}
