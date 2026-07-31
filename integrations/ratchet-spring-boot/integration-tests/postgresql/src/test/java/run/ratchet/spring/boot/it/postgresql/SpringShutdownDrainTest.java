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
package run.ratchet.spring.boot.it.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import run.ratchet.api.JobHandle;
import run.ratchet.spring.boot.it.postgresql.fixture.tck.SpringRatchetTckRuntime;
import run.ratchet.spring.boot.it.postgresql.fixture.tck.TckContexts;
import run.ratchet.tck.api.TckJobs;

/** Spring-local proof that context close drains work before datasource destruction. */
@ExtendWith(OutputCaptureExtension.class)
class SpringShutdownDrainTest extends PostgresqlIntegrationTestSupport {

  @Test
  void contextCloseWaitsForInFlightJobWithoutLateHeartbeatErrors(CapturedOutput output)
      throws Exception {
    TckJobs.resetAll();
    ConfigurableApplicationContext context = TckContexts.start();
    ExecutorService closer = Executors.newSingleThreadExecutor();
    try {
      SpringRatchetTckRuntime runtime = context.getBean(SpringRatchetTckRuntime.class);
      runtime.clear();
      CountDownLatch started = TckJobs.beginBlocking();
      JobHandle handle = runtime.scheduler().enqueueNow(TckJobs::blockUntilReleased);
      runtime.probe().track(handle);
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

      Future<?> close = closer.submit(context::close);
      assertThatThrownBy(() -> close.get(250, TimeUnit.MILLISECONDS))
          .isInstanceOf(TimeoutException.class);

      TckJobs.release();
      close.get(30, TimeUnit.SECONDS);
      assertThat(runtime.probe().awaitCompleted(handle, Duration.ofSeconds(5))).isTrue();
      assertThat(output.getAll())
          .doesNotContain("Heartbeat failed")
          .doesNotContain("Heartbeat retry failed")
          .doesNotContain("EntityManagerFactory is closed");
    } finally {
      TckJobs.release();
      if (context.isActive()) {
        context.close();
      }
      closer.shutdownNow();
      assertThat(closer.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
  }
}
