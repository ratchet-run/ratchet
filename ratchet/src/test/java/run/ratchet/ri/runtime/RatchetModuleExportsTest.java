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
package run.ratchet.ri.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.lang.module.ModuleDescriptor;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class RatchetModuleExportsTest {

  @Test
  void exportsOnlyTheRuntimePackage() throws IOException, URISyntaxException {
    ModuleDescriptor descriptor = readModuleDescriptor();

    assertEquals("run.ratchet.ri", descriptor.name());
    assertEquals(
        Set.of("run.ratchet.ri.runtime"),
        descriptor.exports().stream()
            .map(ModuleDescriptor.Exports::source)
            .collect(Collectors.toSet()));
  }

  private static ModuleDescriptor readModuleDescriptor() throws IOException, URISyntaxException {
    Path artifact =
        Path.of(RatchetRuntime.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    if (Files.isDirectory(artifact)) {
      try (InputStream input = Files.newInputStream(artifact.resolve("module-info.class"))) {
        return ModuleDescriptor.read(input);
      }
    }

    try (JarFile jar = new JarFile(artifact.toFile())) {
      JarEntry entry = jar.getJarEntry("module-info.class");
      try (InputStream input = jar.getInputStream(entry)) {
        return ModuleDescriptor.read(input);
      }
    }
  }
}
