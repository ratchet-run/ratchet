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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Runs a real Boot application in a separate JVM using the consumer test runtime classpath. */
public final class RuntimeProcess implements AutoCloseable {
  public final Process process;
  public final Path log;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
  private int port;

  public RuntimeProcess(Class<?> main, Map<String, ?> properties, String name) throws IOException {
    log = Path.of("target", "runtime-" + name + ".log");
    var command =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty(
                    "surefire.test.class.path", System.getProperty("java.class.path")),
                main.getName()));
    properties.forEach((key, value) -> command.add("--" + key + "=" + value));
    process =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
  }

  public RuntimeProcess ready() throws Exception {
    try {
      return awaitReady();
    } catch (Exception | AssertionError failure) {
      try {
        close();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private RuntimeProcess awaitReady() throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(75).toNanos();
    while (process.isAlive() && System.nanoTime() < deadline) {
      for (String line : Files.readAllLines(log)) {
        if (line.startsWith("RATCHET_RUNTIME_READY ")) {
          port = Integer.parseInt(line.substring("RATCHET_RUNTIME_READY ".length()).trim());
          return this;
        }
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Application failed to become ready:\n" + Files.readString(log));
  }

  public String post(String path) throws Exception {
    var response =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(20))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) throw new HttpFailure(response.statusCode(), response.body());
    return response.body();
  }

  public static final class HttpFailure extends AssertionError {
    public final int status;
    public final String body;

    public HttpFailure(int status, String body) {
      super("HTTP " + status + ": " + body);
      this.status = status;
      this.body = body;
    }
  }

  public void kill() throws Exception {
    process.destroyForcibly();
    if (!process.waitFor(15, TimeUnit.SECONDS)) throw new AssertionError("Worker did not die");
  }

  @Override
  public void close() throws Exception {
    process.destroy();
    if (!process.waitFor(15, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      if (!process.waitFor(10, TimeUnit.SECONDS))
        throw new AssertionError("Cannot stop " + process.pid());
    }
  }
}
