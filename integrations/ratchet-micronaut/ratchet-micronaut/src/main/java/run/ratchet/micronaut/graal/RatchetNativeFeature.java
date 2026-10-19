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
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeClassInitialization;
import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.graalvm.nativeimage.hosted.RuntimeResourceAccess;
import org.graalvm.nativeimage.hosted.RuntimeSerialization;

/** Engine metadata and annotation-free application discovery at image build time. */
public final class RatchetNativeFeature implements Feature {
  @Override
  public void beforeAnalysis(BeforeAnalysisAccess access) {
    NativeImageSupport.registerType(access, "run.ratchet.store.entity.JobPayload");
    registerMethod(
        access,
        "run.ratchet.ri.cdi.RecurringMethodInvoker",
        "invoke",
        String.class,
        String.class,
        boolean.class);
    registerMethod(access, "run.ratchet.ri.util.JobPlaceholders", "noop");
    registerMethod(access, "java.util.concurrent.Executors", "newVirtualThreadPerTaskExecutor");
    for (String name :
        List.of("org.eclipse.yasson.JsonBindingProvider", "org.eclipse.parsson.JsonProviderImpl")) {
      Class<?> type = access.findClassByName(name);
      if (type != null) {
        RuntimeReflection.register(type);
        RuntimeReflection.registerAllDeclaredConstructors(type);
        NativeImageSupport.registerMembers(type);
      }
    }
    Class<?> uuid = access.findClassByName("run.ratchet.store.id.UuidV7Factory");
    if (uuid != null) RuntimeClassInitialization.initializeAtRunTime(uuid);
    NativeImageSupport.registerResources(
        access,
        RatchetNativeFeature.class.getModule(),
        name ->
            name.equals("META-INF/services/jakarta.json.bind.spi.JsonbProvider")
                || name.equals("META-INF/services/jakarta.json.spi.JsonProvider"));
    // RatchetTransactionBoundaryInterceptor resolves each call with getMethod on these subclasses
    // and reads the jakarta @Transactional they inherit from the core services.
    NativeImageSupport.visitEntries(
        access.getApplicationClassPath(),
        name ->
            name.startsWith("run/ratchet/micronaut/internal/tx/")
                && name.endsWith(".class")
                && !name.contains("$")
                && !name.endsWith("package-info.class"),
        (resource, bytes) ->
            registerTransactionBoundary(
                access, resource.substring(0, resource.length() - 6).replace('/', '.')));

    Map<String, byte[]> classes = new LinkedHashMap<>();
    Map<String, Path> origins = new LinkedHashMap<>();
    Map<Path, Set<String>> entriesByPath = new LinkedHashMap<>();
    for (Path path : access.getApplicationClassPath()) {
      Set<String> entries = new LinkedHashSet<>();
      entriesByPath.put(path, entries);
      NativeImageSupport.visitEntries(
          List.of(path),
          resource ->
              resource.endsWith(".class")
                  && ApplicationDiscovery.isApplicationClass(
                      resource.substring(0, resource.length() - 6).replace('/', '.')),
          (resource, bytes) -> {
            String name = resource.substring(0, resource.length() - 6).replace('/', '.');
            if (classes.putIfAbsent(name, bytes) == null) {
              origins.put(name, path);
              entries.add(name);
            }
          });
    }
    Map<String, ApplicationDiscovery.Discovery> parsed = new LinkedHashMap<>();
    var roots = ApplicationDiscovery.roots(classes);
    var pending = new ArrayDeque<>(roots);
    int seeds = roots.size();
    Set<String> registered = new LinkedHashSet<>();
    int lambdas = 0;
    while (!pending.isEmpty()) {
      String name = pending.removeFirst();
      if (!ApplicationDiscovery.isApplicationClass(name) || !registered.add(name)) continue;
      Class<?> type = NativeImageSupport.registerType(access, name);
      if (type == null) {
        registered.remove(name);
        continue;
      }
      Class<?> superclass = type.getSuperclass();
      if (superclass != null) pending.add(superclass.getName());
      for (Class<?> implemented : type.getInterfaces()) pending.add(implemented.getName());
      String resource = name.replace('.', '/') + ".class";
      byte[] bytes = classes.get(name);
      if (bytes == null) {
        try (var input = access.getApplicationClassLoader().getResourceAsStream(resource)) {
          if (input != null) bytes = input.readAllBytes();
        } catch (IOException failure) {
          throw new IllegalStateException("Cannot inspect application class " + name, failure);
        }
      }
      if (bytes != null) {
        RuntimeResourceAccess.addResource(type.getModule(), resource, bytes);
        byte[] classBytes = bytes;
        var discovery =
            parsed.computeIfAbsent(name, ignored -> ApplicationDiscovery.discover(classBytes));
        pending.addAll(
            ApplicationDiscovery.relatedClasses(
                name, discovery, entriesByPath.getOrDefault(origins.get(name), Set.of())));
        if (discovery.lambdaCapturing()) {
          RuntimeSerialization.registerLambdaCapturingClass(type);
          lambdas++;
        }
      }
      if (Boolean.getBoolean("ratchet.native.verbose"))
        System.out.println("[ratchet-native] " + name);
    }
    System.out.println(
        "[ratchet-native] "
            + seeds
            + " submitter candidates, "
            + registered.size()
            + " application types, "
            + lambdas
            + " lambda-capturing classes");
  }

  private static void registerTransactionBoundary(BeforeAnalysisAccess access, String name) {
    Class<?> type = access.findClassByName(name);
    if (type == null) return;
    RuntimeReflection.register(type.getMethods());
    for (Class<?> current = type; current != Object.class; current = current.getSuperclass()) {
      RuntimeReflection.register(current);
    }
  }

  private static void registerMethod(
      BeforeAnalysisAccess access, String className, String method, Class<?>... parameters) {
    Class<?> type = access.findClassByName(className);
    if (type == null) return;
    try {
      RuntimeReflection.register(type);
      RuntimeReflection.register(type.getDeclaredMethod(method, parameters));
    } catch (NoSuchMethodException failure) {
      // Virtual threads are optional when the consuming application targets Java 17.
      if (className.equals("java.util.concurrent.Executors")) return;
      throw new IllegalStateException(
          "Ratchet native metadata no longer matches " + className + "#" + method, failure);
    }
  }
}
