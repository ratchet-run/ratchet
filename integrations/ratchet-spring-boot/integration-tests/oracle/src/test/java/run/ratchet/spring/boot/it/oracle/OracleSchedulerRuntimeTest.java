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
package run.ratchet.spring.boot.it.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.spring.boot.it.oracle.fixture.tck.TckApplication;
import run.ratchet.spring.boot.it.oracle.fixture.tck.TckApplicationContextInitializer;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.tck.api.ListenerProbe;

/** End-to-end proof that the starter owns a complete Oracle scheduler runtime. */
@ExtendWith(OutputCaptureExtension.class)
public class OracleSchedulerRuntimeTest extends OracleIntegrationTestSupport {

  private static final AtomicReference<RuntimePayload> RECEIVED_PAYLOAD = new AtomicReference<>();

  @BeforeEach
  void resetRuntimePayload() {
    RECEIVED_PAYLOAD.set(null);
  }

  @Test
  void runnerExecutesAfterMigratedRuntimeStartAndContextCloseDrains(CapturedOutput output) {
    SmartLifecycle lifecycle;
    RuntimeEvidence evidence;
    SqlStatementProbe sqlProbe;

    try (ConfigurableApplicationContext context =
        new SpringApplicationBuilder(
                TckApplication.class, RuntimeTestConfiguration.class, SqlProbeConfiguration.class)
            .web(WebApplicationType.NONE)
            .registerShutdownHook(false)
            .initializers(new TckApplicationContextInitializer())
            .run("--spring.main.banner-mode=off")) {
      evidence = context.getBean(RuntimeEvidence.class);
      lifecycle = context.getBean("ratchetLifecycle", SmartLifecycle.class);
      sqlProbe = context.getBean(SqlStatementProbe.class);

      assertThat(evidence.schemaMigratedBeforeRuntimeStart()).isTrue();
      assertThat(evidence.runnerObservedStartedLifecycle()).isTrue();
      assertThat(evidence.serializerInstallationWasLive()).isTrue();
      assertThat(evidence.jobCompleted()).isTrue();
      assertThat(RECEIVED_PAYLOAD.get()).isEqualTo(evidence.submittedPayload());
      assertThat(lifecycle.isRunning()).isTrue();

      int migrationIndex = sqlProbe.lastIndexContaining("merge into ratchet_schema_version");
      int nodeStartIndex = sqlProbe.firstIndexContaining("merge into scheduler_node");
      assertThat(migrationIndex).isGreaterThanOrEqualTo(0);
      assertThat(nodeStartIndex).isGreaterThan(migrationIndex);
    }

    assertThat(lifecycle.isRunning()).isFalse();
    assertThat(output.getAll())
        .doesNotContain("Heartbeat failed")
        .doesNotContain("Heartbeat retry failed")
        .doesNotContain("EntityManagerFactory is closed");
  }

  /** Public static job target so the invocation survives persistence and worker reconstruction. */
  public static void receivePayload(RuntimePayload payload) {
    RECEIVED_PAYLOAD.set(payload);
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class RuntimeTestConfiguration {

    @Bean
    RuntimeEvidence runtimeEvidence() {
      return new RuntimeEvidence();
    }

    @Bean
    SchedulerLifecycleHook migrationOrderingHook(DataSource dataSource, RuntimeEvidence evidence) {
      JdbcTemplate jdbc = new JdbcTemplate(dataSource);
      return new SchedulerLifecycleHook() {
        @Override
        public void beforeStart() {
          Long migrationCount =
              jdbc.queryForObject("SELECT COUNT(*) FROM ratchet_schema_version", Long.class);
          Long schedulerTableCount =
              jdbc.queryForObject(
                  "SELECT COUNT(*) FROM user_tables WHERE table_name = 'SCHEDULER_JOB'",
                  Long.class);
          evidence.schemaMigratedBeforeRuntimeStart =
              migrationCount != null
                  && migrationCount > 0L
                  && schedulerTableCount != null
                  && schedulerTableCount == 1L;
        }
      };
    }

    @Bean
    ApplicationRunner qualifyingRunner(
        ApplicationContext applicationContext,
        JobSchedulerService scheduler,
        ListenerProbe probe,
        PayloadSerializer payloadSerializer,
        RuntimeEvidence evidence) {
      return arguments -> {
        SmartLifecycle lifecycle =
            applicationContext.getBean("ratchetLifecycle", SmartLifecycle.class);
        evidence.runnerObservedStartedLifecycle = lifecycle.isRunning();
        evidence.serializerInstallationWasLive = PayloadSerializerHolder.get() == payloadSerializer;

        RuntimePayload payload = new RuntimePayload("spring-oracle", 8);
        evidence.submittedPayload = payload;
        JobHandle handle = scheduler.enqueueNow(() -> receivePayload(payload));
        probe.track(handle);
        evidence.jobCompleted = probe.awaitCompleted(handle, Duration.ofSeconds(30));
      };
    }
  }

  static final class RuntimeEvidence {

    private volatile boolean schemaMigratedBeforeRuntimeStart;
    private volatile boolean runnerObservedStartedLifecycle;
    private volatile boolean serializerInstallationWasLive;
    private volatile boolean jobCompleted;
    private volatile RuntimePayload submittedPayload;

    boolean schemaMigratedBeforeRuntimeStart() {
      return schemaMigratedBeforeRuntimeStart;
    }

    boolean runnerObservedStartedLifecycle() {
      return runnerObservedStartedLifecycle;
    }

    boolean serializerInstallationWasLive() {
      return serializerInstallationWasLive;
    }

    boolean jobCompleted() {
      return jobCompleted;
    }

    RuntimePayload submittedPayload() {
      return submittedPayload;
    }
  }

  /** Conservative bean-style JSON-B payload used by both supported Boot lanes. */
  public static final class RuntimePayload implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private String name;
    private int sequence;

    public RuntimePayload() {}

    RuntimePayload(String name, int sequence) {
      this.name = name;
      this.sequence = sequence;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getSequence() {
      return sequence;
    }

    public void setSequence(int sequence) {
      this.sequence = sequence;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof RuntimePayload that)) {
        return false;
      }
      return sequence == that.sequence && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, sequence);
    }
  }
}
