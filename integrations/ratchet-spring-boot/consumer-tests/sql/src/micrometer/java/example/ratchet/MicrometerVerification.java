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
package example.ratchet;

import static example.ratchet.verification.NativeVerification.await;
import static example.ratchet.verification.NativeVerification.check;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.SchedulerLifecycleHook;

@Configuration(proxyBeanMethods = false)
@Profile("verification")
public class MicrometerVerification {
  static volatile boolean startupVerified;

  @Bean
  ApplicationRunner verifyMicrometer(
      Environment environment,
      ApplicationContext context,
      MeterRegistry registry,
      JobSchedulerService scheduler) {
    return args -> {
      if (!environment.getProperty("consumer.scenario", "normal").equals("micrometer")) return;
      MetricsCollector collector = context.getBean(MetricsCollector.class);
      check(
          AopUtils.getTargetClass(collector) == MicrometerMetricsCollector.class,
          "unexpected MetricsCollector: " + AopUtils.getTargetClass(collector).getName());
      scheduler
          .schedule(Duration.ofHours(1), MicrometerVerification::idle)
          .withBusinessKey("micrometer-pending")
          .submit();
      scheduler.enqueue(MicrometerVerification::idle).submit();
      await(
          () -> {
            var gauge = registry.find("ratchet.queue.jobs").tag("status", "PENDING").gauge();
            if (gauge == null) return false;
            double value = gauge.value();
            return Double.isFinite(value) && value >= 1;
          },
          "PENDING queue gauge did not report a pending job");
      await(
          () -> {
            var counter = registry.find("ratchet.jobs.completed").counter();
            return counter != null && counter.count() >= 1;
          },
          "Micrometer collector did not record a completed job");
      startupVerified = true;
    };
  }

  @Bean
  QueueGaugeShutdownCheck queueGaugeShutdownCheck(Environment environment, MeterRegistry registry) {
    return new QueueGaugeShutdownCheck(environment, registry);
  }

  public static void idle() {}

  public static class QueueGaugeShutdownCheck implements SchedulerLifecycleHook {
    private final Environment environment;
    private final MeterRegistry registry;

    QueueGaugeShutdownCheck(Environment environment, MeterRegistry registry) {
      this.environment = environment;
      this.registry = registry;
    }

    @Override
    public void afterStop() {
      if (!environment.getProperty("consumer.scenario", "normal").equals("micrometer")) return;
      var remaining =
          registry.getMeters().stream()
              .map(meter -> meter.getId().getName())
              .filter(name -> name.startsWith("ratchet.queue."))
              .toList();
      if (startupVerified && remaining.isEmpty()) {
        System.out.println("RATCHET_MICROMETER_VERIFIED");
      } else {
        System.out.println(
            "RATCHET_MICROMETER_FAILED startupVerified="
                + startupVerified
                + " remaining="
                + remaining);
      }
    }
  }
}
