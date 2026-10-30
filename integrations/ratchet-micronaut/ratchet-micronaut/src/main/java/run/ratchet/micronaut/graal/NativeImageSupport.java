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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import org.graalvm.nativeimage.hosted.Feature.FeatureAccess;
import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.graalvm.nativeimage.hosted.RuntimeResourceAccess;

/** Shared build-time utilities for the optional Ratchet native features. */
public final class NativeImageSupport {
  private NativeImageSupport() {}

  /** Visits selected entries without expanding jars or loading their classes. */
  public static void visitEntries(
      List<Path> paths, Predicate<String> selected, BiConsumer<String, byte[]> visitor) {
    for (Path path : paths) {
      try {
        if (Files.isDirectory(path)) {
          try (var files = Files.walk(path)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
              String name = path.relativize(file).toString().replace('\\', '/');
              if (selected.test(name)) visitor.accept(name, Files.readAllBytes(file));
            }
          }
        } else if (Files.isRegularFile(path)) {
          try (var jar = new JarFile(path.toFile())) {
            var entries = jar.entries();
            while (entries.hasMoreElements()) {
              var entry = entries.nextElement();
              // The root class is the Java 17 contract; avoid treating multi-release overrides
              // as separate classes or registering the wrong binary name.
              if (!entry.isDirectory()
                  && !entry.getName().startsWith("META-INF/versions/")
                  && selected.test(entry.getName())) {
                try (var input = jar.getInputStream(entry)) {
                  visitor.accept(entry.getName(), input.readAllBytes());
                }
              }
            }
          }
        }
      } catch (IOException failure) {
        throw new IllegalStateException(
            "Cannot inspect native-image classpath entry " + path, failure);
      }
    }
  }

  /** Registers only a type available in this application's image classpath. */
  public static Class<?> registerType(FeatureAccess access, String name) {
    Class<?> type = access.findClassByName(name);
    if (type != null) {
      RuntimeReflection.register(type);
      RuntimeReflection.registerAllDeclaredConstructors(type);
      RuntimeReflection.registerAllDeclaredMethods(type);
      RuntimeReflection.registerAllDeclaredFields(type);
      if (type.isRecord()) RuntimeReflection.registerAllRecordComponents(type);
      registerMembers(type);
    }
    return type;
  }

  /**
   * Registers every declared constructor, method and field for invocation and access. The {@code
   * registerAll*} calls only make members visible to reflective lookup; calling one still fails
   * with {@code MissingReflectionRegistrationError} unless the member itself is registered.
   */
  public static void registerMembers(Class<?> type) {
    try {
      RuntimeReflection.register(type.getDeclaredConstructors());
      RuntimeReflection.register(type.getDeclaredMethods());
      RuntimeReflection.register(type.getDeclaredFields());
    } catch (LinkageError e) {
      // A member signature names a class that is not on the image class path. The lookup hints
      // above still apply.
    }
  }

  /** Includes every occurrence of a matching resource, including vendor migration collisions. */
  public static void registerResources(
      FeatureAccess access, Module module, Predicate<String> selected) {
    visitEntries(
        access.getApplicationClassPath(),
        selected,
        (name, bytes) -> RuntimeResourceAccess.addResource(module, name, bytes));
  }
}
