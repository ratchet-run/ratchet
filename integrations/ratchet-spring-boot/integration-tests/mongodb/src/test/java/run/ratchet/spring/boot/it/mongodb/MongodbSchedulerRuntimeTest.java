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
package run.ratchet.spring.boot.it.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoDatabase;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
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
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.TckApplication;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.TckApplicationContextInitializer;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.tck.api.ListenerProbe;

/** End-to-end proof that the starter owns a complete MongoDB scheduler runtime. */
@ExtendWith(OutputCaptureExtension.class)
public class MongodbSchedulerRuntimeTest {

  private static final AtomicReference<RuntimePayload> RECEIVED_PAYLOAD = new AtomicReference<>();

  @BeforeEach
  void resetRuntimePayload() {
    RECEIVED_PAYLOAD.set(null);
  }

  @Test
  void runnerExecutesAfterRuntimeStartAndContextCloseDrains(CapturedOutput output) {
    SmartLifecycle lifecycle;
    RuntimeEvidence evidence;

    try (ConfigurableApplicationContext context =
        new SpringApplicationBuilder(TckApplication.class, RuntimeTestConfiguration.class)
            .web(WebApplicationType.NONE)
            .registerShutdownHook(false)
            .initializers(new TckApplicationContextInitializer())
            .run("--spring.main.banner-mode=off")) {
      evidence = context.getBean(RuntimeEvidence.class);
      lifecycle = context.getBean("ratchetLifecycle", SmartLifecycle.class);
      MongoDatabase database = context.getBean(MongoDatabase.class);

      assertThat(evidence.runnerObservedStartedLifecycle()).isTrue();
      assertThat(evidence.serializerInstallationWasLive()).isTrue();
      assertThat(evidence.jobCompleted()).isTrue();
      assertThat(RECEIVED_PAYLOAD.get()).isEqualTo(evidence.submittedPayload());
      assertThat(lifecycle.isRunning()).isTrue();

      ArrayList<String> collectionNames = database.listCollectionNames().into(new ArrayList<>());
      ArrayList<Document> schedulerJobIndexes =
          database.getCollection("scheduler_job").listIndexes().into(new ArrayList<>());
      assertThat(collectionNames).contains("scheduler_job");
      assertThat(schedulerJobIndexes).hasSizeGreaterThan(1);
    }

    assertThat(lifecycle.isRunning()).isFalse();
    assertThat(output.getAll())
        .doesNotContain("Heartbeat failed")
        .doesNotContain("Heartbeat retry failed");
  }

  /** Public static job target so invocation survives persistence and worker reconstruction. */
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
    ApplicationRunner qualifyingRunner(
        ApplicationContext applicationContext,
        JobSchedulerService scheduler,
        ListenerProbe probe,
        PayloadSerializer payloadSerializer,
        RuntimeEvidence evidence) {
      return arguments -> {
        SmartLifecycle lifecycle =
            applicationContext.getBean("ratchetLifecycle", SmartLifecycle.class);
        assertThat(lifecycle.isRunning()).isTrue();
        assertThat(PayloadSerializerHolder.get()).isSameAs(payloadSerializer);
        evidence.runnerObservedStartedLifecycle = true;
        evidence.serializerInstallationWasLive = true;

        RuntimePayload payload = new RuntimePayload("spring-mongodb", 9);
        evidence.submittedPayload = payload;
        JobHandle handle = scheduler.enqueueNow(() -> receivePayload(payload));
        probe.track(handle);
        evidence.jobCompleted = probe.awaitCompleted(handle, Duration.ofSeconds(30));
      };
    }
  }

  static final class RuntimeEvidence {

    private volatile boolean runnerObservedStartedLifecycle;
    private volatile boolean serializerInstallationWasLive;
    private volatile boolean jobCompleted;
    private volatile RuntimePayload submittedPayload;

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
