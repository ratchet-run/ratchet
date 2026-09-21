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
package run.ratchet.consumer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** JVM test harness launching either the packaged application or its compiled native executable. */
public final class ConsumerProcess {
  private ConsumerProcess() {}

  public static List<String> command(String application, String... arguments) {
    List<String> command = new ArrayList<>();
    if (Boolean.getBoolean("consumer.native")) {
      command.add(Path.of("target", application).toAbsolutePath().toString());
      command.add("-XX:MissingRegistrationReportingMode=Exit");
    } else {
      command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
      if (Boolean.getBoolean("consumer.aot")) command.add("-Dspring.aot.enabled=true");
      command.add("-jar");
      command.add("target/" + application + "-1.0-SNAPSHOT.jar");
    }
    command.add("--spring.profiles.include=verification");
    command.add("--ratchet.allowed-packages=example.ratchet,example.library");
    command.addAll(List.of(arguments));
    return command;
  }
}
