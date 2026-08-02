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
package run.ratchet.spring.boot.it.nativepostgresql;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.runtime.RatchetRuntimeDefaults;
import run.ratchet.spi.ClassPolicy;

/** Native-image entry point for the real PostgreSQL-backed Ratchet scheduler qualification. */
@SpringBootApplication
public class NativePostgresqlApplication {

  public static void main(String[] args) {
    try (ConfigurableApplicationContext ignored =
        SpringApplication.run(NativePostgresqlApplication.class, args)) {
      // The qualifying runner has completed and stopped the Ratchet lifecycle before run returns.
    }
  }

  /**
   * Preserves the configured package policy while denying one AOT-reachable application job.
   *
   * <p>The denied type deliberately remains inside the configured package. This makes the Spring
   * AOT processor register it for native reachability, so both rejection scenarios exercise this
   * policy rather than a missing-manifest or missing-reflection path.
   */
  @Bean
  ClassPolicy nativePostgresqlClassPolicy(RatchetOptions options) {
    ClassPolicy delegate = RatchetRuntimeDefaults.classPolicy(options);
    return new ClassPolicy() {
      @Override
      public boolean isAllowed(String className) {
        return !DeniedJob.class.getName().equals(className) && delegate.isAllowed(className);
      }

      @Override
      public boolean isAllowedForResultType(String className) {
        return delegate.isAllowedForResultType(className);
      }
    };
  }

  @Bean
  @ConditionalOnProperty(
      name = "native-postgresql.run-on-startup",
      havingValue = "true",
      matchIfMissing = true)
  ApplicationRunner nativePostgresqlRunner(NativePostgresqlScenarios scenarios) {
    return arguments -> {
      var evidence = scenarios.runAll();
      evidence.forEach(
          scenario -> System.out.println("SPRING_BOOT_NATIVE_EVIDENCE=" + scenario.toJson()));
      if (evidence.stream().anyMatch(scenario -> !scenario.passed())) {
        throw new IllegalStateException("Spring Boot native PostgreSQL qualification failed");
      }
    };
  }
}
