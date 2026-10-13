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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Launches only application classes and runtime dependencies, never the test classpath. */
public final class ConsumerProcess implements AutoCloseable {
  private final Process process;
  private final Path log;

  private ConsumerProcess(Process process, Path log) {
    this.process = process;
    this.log = log;
  }

  public static ConsumerProcess start(String application, Map<String, Object> properties, Path log)
      throws Exception {
    List<String> command = new ArrayList<>();
    boolean nativeImage = Boolean.getBoolean("consumer.native");
    command.add(
        nativeImage
            ? Path.of("target", application).toAbsolutePath().toString()
            : Path.of(System.getProperty("java.home"), "bin", "java").toString());
    if (nativeImage) command.add("-XX:MissingRegistrationReportingMode=Exit");
    properties.forEach((key, value) -> command.add("-D" + key + "=" + value));
    if (!nativeImage) {
      command.add("-cp");
      command.add(
          Path.of("target/classes").toAbsolutePath()
              + System.getProperty("path.separator")
              + Files.readString(Path.of("target/runtime-classpath.txt")).trim());
      command.add(
          application.equals("sql-consumer")
              ? "example.ratchet.ConsumerApplication"
              : "example.ratchet.mongo.MongoConsumerApplication");
    }
    return new ConsumerProcess(
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start(),
        log);
  }

  public String output() throws Exception {
    return Files.readString(log);
  }

  public void awaitMarker(String marker) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(100).toNanos();
    while (!output().contains(marker)) {
      if (!process.isAlive() || System.nanoTime() >= deadline)
        throw new AssertionError("Missing " + marker + ": " + output());
      Thread.sleep(100);
    }
  }

  public int awaitExit() throws Exception {
    return awaitExit(120);
  }

  public int awaitExit(int seconds) throws Exception {
    if (!process.waitFor(seconds, TimeUnit.SECONDS))
      throw new AssertionError("Timeout: " + output());
    return process.exitValue();
  }

  public void terminate() {
    process.destroy();
  }

  public void kill() throws Exception {
    process.destroyForcibly();
    if (!process.waitFor(20, TimeUnit.SECONDS)) throw new AssertionError("Cannot kill consumer");
  }

  @Override
  public void close() throws Exception {
    kill();
  }
}
