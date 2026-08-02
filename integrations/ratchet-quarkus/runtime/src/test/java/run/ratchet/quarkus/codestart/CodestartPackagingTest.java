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
package run.ratchet.quarkus.codestart;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class CodestartPackagingTest {

  @Test
  void stagedCodestartIncludesConcreteRatchetStoreVersion() throws IOException {
    Path codestart =
        Paths.get(
            "target",
            "codestarts-staging",
            "codestarts",
            "quarkus",
            "ratchet-codestart",
            "codestart.yml");

    assertTrue(
        Files.exists(codestart),
        () -> "Expected staged codestart.yml to exist at " + codestart.toAbsolutePath());

    String text = Files.readString(codestart);
    String dependencyLine =
        text.lines()
            .filter(line -> line.contains("run.ratchet:ratchet-store-postgresql"))
            .findFirst()
            .orElse("");

    assertFalse(
        dependencyLine.isEmpty(),
        "Expected ratchet-store-postgresql dependency in staged codestart.yml");
    assertFalse(
        dependencyLine.matches(".*run\\.ratchet:ratchet-store-postgresql\\s*$"),
        "Expected ratchet-store-postgresql dependency to include a version, but found bare dependency: "
            + dependencyLine);
    assertTrue(
        dependencyLine.matches(".*run\\.ratchet:ratchet-store-postgresql:[^\\s]+.*"),
        "Expected ratchet-store-postgresql dependency to include a concrete version, but found: "
            + dependencyLine);
    assertFalse(
        text.contains("${"),
        "Expected staged codestart.yml to have no unfiltered Maven placeholders");
  }
}
