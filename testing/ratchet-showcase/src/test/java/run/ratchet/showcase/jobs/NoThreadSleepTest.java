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
package run.ratchet.showcase.jobs;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class NoThreadSleepTest {

  @Test
  void showcaseJobCodeDoesNotSleepThreads() throws IOException {
    Path root = Path.of("src/main/java/run/ratchet/showcase");
    boolean usesSleep;
    try (var paths = Files.walk(root)) {
      usesSleep =
          paths
              .filter(path -> path.toString().endsWith(".java"))
              .map(NoThreadSleepTest::read)
              .anyMatch(source -> source.contains("Thread.sleep"));
    }

    assertFalse(usesSleep, "Showcase job code must use scheduling, signals, and retries instead");
  }

  private static String read(Path path) {
    try {
      return Files.readString(path);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
