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
package run.ratchet.spring.boot.it.aotpreflight;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/** Native-image entry point for the Ratchet Spring AOT payload preflight. */
@SpringBootApplication
public class AotPreflightApplication {

  public static void main(String[] args) {
    boolean passed;
    try (ConfigurableApplicationContext context =
        SpringApplication.run(AotPreflightApplication.class, args)) {
      var evidence = context.getBean(AotPreflightScenarios.class).runAll();
      evidence.forEach(
          scenario -> System.out.println("SPRING_BOOT_AOT_EVIDENCE=" + scenario.toJson()));
      passed = evidence.stream().allMatch(AotPreflightScenarios.Evidence::passed);
    }
    if (!passed) {
      throw new IllegalStateException("Spring Boot AOT payload preflight failed");
    }
  }
}
