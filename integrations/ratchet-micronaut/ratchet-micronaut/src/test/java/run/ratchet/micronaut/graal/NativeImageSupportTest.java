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
package run.ratchet.micronaut.graal;

import static org.junit.jupiter.api.Assertions.*;

import example.nativejobs.DiscoveryFixtures;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NativeImageSupportTest {
  @TempDir Path temporary;

  @Test
  void scansCompiledFixturesInBothDirectoriesAndJars() throws Exception {
    Class<?> fixture = DiscoveryFixtures.MethodReferenceSubmitter.class;
    String resource = fixture.getName().replace('.', '/') + ".class";
    byte[] bytes;
    try (var input = fixture.getResourceAsStream("/" + resource)) {
      assertNotNull(input);
      bytes = input.readAllBytes();
    }
    Path directory = temporary.resolve("classes");
    Path file = directory.resolve(resource);
    Files.createDirectories(file.getParent());
    Files.write(file, bytes);
    Path jar = temporary.resolve("fixtures.jar");
    try (var output = new JarOutputStream(Files.newOutputStream(jar))) {
      output.putNextEntry(new JarEntry(resource));
      output.write(bytes);
      output.closeEntry();
      output.putNextEntry(new JarEntry("META-INF/versions/25/" + resource));
      output.write(bytes);
      output.closeEntry();
    }
    var names = new ArrayList<String>();
    NativeImageSupport.visitEntries(
        List.of(directory, jar),
        name -> name.endsWith(".class"),
        (name, content) -> {
          assertArrayEquals(bytes, content);
          names.add(ApplicationDiscovery.discover(content).name());
        });
    assertEquals(List.of(fixture.getName(), fixture.getName()), names);
  }

  @Test
  void keepsDuplicateMigrationNamesWithTheirDistinctVendorContents() throws Exception {
    List<Path> jars = new ArrayList<>();
    for (String vendor : List.of("postgresql", "mysql")) {
      Path jar = temporary.resolve(vendor + ".jar");
      try (var output = new JarOutputStream(Files.newOutputStream(jar))) {
        output.putNextEntry(new JarEntry("ddl/migrations/postgresql/index.txt"));
        output.write(vendor.getBytes(StandardCharsets.UTF_8));
        output.closeEntry();
        output.putNextEntry(new JarEntry("unrelated.txt"));
        output.closeEntry();
      }
      jars.add(jar);
    }
    var contents = new ArrayList<String>();
    NativeImageSupport.visitEntries(
        jars,
        name -> name.startsWith("ddl/migrations/"),
        (name, bytes) -> {
          assertEquals("ddl/migrations/postgresql/index.txt", name);
          contents.add(new String(bytes, StandardCharsets.UTF_8));
        });
    assertEquals(List.of("postgresql", "mysql"), contents);
  }
}
