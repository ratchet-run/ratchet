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
package run.ratchet.spring.boot.autoconfigure;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aot.hint.BindingReflectionHintsRegistrar;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotContribution;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotProcessor;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.util.ClassUtils;

/** Inspects definitions and class files only: no application beans are obtained during AOT. */
public final class RatchetApplicationAotProcessor implements BeanFactoryInitializationAotProcessor {
  private static final String NATIVE_PATH = "META-INF/native-image/run.ratchet/spring/";
  private static final Log logger = LogFactory.getLog(RatchetApplicationAotProcessor.class);

  @Override
  public BeanFactoryInitializationAotContribution processAheadOfTime(
      ConfigurableListableBeanFactory beanFactory) {
    if (beanFactory.getBeanNamesForType(RatchetAutoConfiguration.class, true, false).length == 0)
      return null;
    Map<String, Class<?>> types = discover(beanFactory);
    return (generation, initialization) -> {
      RuntimeHints hints = generation.getRuntimeHints();
      new RatchetRuntimeHints().registerHints(hints, beanFactory.getBeanClassLoader());
      types.values().forEach(type -> register(hints, type));
      // Spring's SerializationHints models Serializable types, not lambda capturing types.
      // Keep lambda serialization and the bytecode used by Ratchet's analyzer in the same pass.
      String lambdas =
          types.values().stream()
              .filter(
                  type ->
                      Arrays.stream(type.getDeclaredMethods())
                          .anyMatch(method -> method.getName().equals("$deserializeLambda$")))
              .map(Class::getName)
              .map(name -> "{\"name\":\"" + name + "\"}")
              .collect(Collectors.joining(",\n"));
      generation
          .getGeneratedFiles()
          .addResourceFile(
              NATIVE_PATH + "serialization-config.json",
              "{\"types\":[],\"lambdaCapturingTypes\":[" + lambdas + "],\"proxies\":[]}\n");
      generation
          .getGeneratedFiles()
          .addResourceFile(
              NATIVE_PATH + "native-image.properties",
              "Args = --initialize-at-run-time=run.ratchet.store.id.UuidV7Factory\n");
    };
  }

  static Map<String, Class<?>> discover(ConfigurableListableBeanFactory factory) {
    ClassLoader loader = factory.getBeanClassLoader();
    Map<String, Class<?>> types = new TreeMap<>();
    var packages = new TreeSet<String>();
    if (AutoConfigurationPackages.has(factory))
      packages.addAll(AutoConfigurationPackages.get(factory));
    for (String name : factory.getBeanDefinitionNames()) {
      Class<?> type = factory.getType(name, false);
      if (type == null) continue;
      type = ClassUtils.getUserClass(type);
      if (factory.getBeanDefinition(name).getRole() == BeanDefinition.ROLE_APPLICATION
          && !frameworkType(type)) {
        types.put(type.getName(), type);
        scanPattern(type.getName().replace('.', '/') + "$*.class", types, loader);
      }
      addExplicit(type, types, packages, loader);
    }
    var scanned = new TreeSet<String>();
    while (!scanned.containsAll(packages)) {
      for (String name : new ArrayList<>(packages)) {
        if (scanned.add(name)) scan(name, types, loader);
      }
      for (Class<?> type : new ArrayList<>(types.values()))
        addExplicit(type, types, packages, loader);
    }
    for (Class<?> type : new ArrayList<>(types.values())) {
      for (Class<?> parent = type.getSuperclass();
          parent != null && !frameworkType(parent);
          parent = parent.getSuperclass()) {
        types.putIfAbsent(parent.getName(), parent);
      }
    }
    return types;
  }

  private static boolean frameworkType(Class<?> type) {
    String name = type.getName();
    return name.startsWith("java.")
        || name.startsWith("javax.")
        || name.startsWith("jakarta.")
        || name.startsWith("org.springframework.")
        || name.startsWith("org.hibernate.")
        || name.startsWith("run.ratchet.spring.")
        || name.startsWith("run.ratchet.ri.")
        || name.startsWith("run.ratchet.store.");
  }

  private static void addExplicit(
      Class<?> source, Map<String, Class<?>> types, TreeSet<String> packages, ClassLoader loader) {
    RegisterRatchetTypes annotation =
        AnnotatedElementUtils.findMergedAnnotation(source, RegisterRatchetTypes.class);
    if (annotation == null) return;
    for (Class<?> type : annotation.value()) {
      types.put(type.getName(), type);
      // getDeclaredClasses misses local and anonymous classes. Scan the lexical class prefix.
      scanPattern(type.getName().replace('.', '/') + "$*.class", types, loader);
    }
    for (Class<?> marker : annotation.basePackageClasses()) packages.add(marker.getPackageName());
  }

  private static void scan(String packageName, Map<String, Class<?>> types, ClassLoader loader) {
    if (packageName.isBlank())
      throw new IllegalStateException("Ratchet AOT requires named application packages");
    scanPattern(packageName.replace('.', '/') + "/**/*.class", types, loader);
  }

  private static void scanPattern(String pattern, Map<String, Class<?>> types, ClassLoader loader) {
    var resources = new PathMatchingResourcePatternResolver(loader);
    var metadata = new CachingMetadataReaderFactory(resources);
    try {
      for (var resource : resources.getResources("classpath*:" + pattern)) {
        String name = metadata.getMetadataReader(resource).getClassMetadata().getClassName();
        if (name.endsWith("package-info")
            || name.endsWith("module-info")
            || name.contains("$$SpringCGLIB$$")
            || name.contains("__BeanDefinitions")
            || name.contains("__BeanFactoryRegistrations")
            || name.contains("__ApplicationContextInitializer")
            || name.contains("__EnvironmentPostProcessor")) continue;
        if (types.containsKey(name)) continue;
        try {
          // A package may also contain inactive configurations for optional integrations.
          // Class.forName alone does not resolve method/field signatures. Check them before
          // admitting a scanned type so an absent dependency cannot poison native metadata.
          // Explicit registrations and actual bean types bypass this best-effort scan and
          // still fail if their required dependencies are missing.
          Class<?> type = Class.forName(name, false, loader);
          for (Class<?> current = type;
              current != null && !frameworkType(current);
              current = current.getSuperclass()) {
            for (var method : current.getDeclaredMethods()) {
              method.getGenericReturnType();
              method.getGenericParameterTypes();
            }
            for (var constructor : current.getDeclaredConstructors())
              constructor.getGenericParameterTypes();
            for (var field : current.getDeclaredFields()) field.getGenericType();
          }
          types.put(name, type);
        } catch (NoClassDefFoundError | TypeNotPresentException unavailable) {
          logger.debug("Skipping unavailable Ratchet AOT scan candidate " + name, unavailable);
        }
      }
    } catch (IOException | ClassNotFoundException | LinkageError failure) {
      throw new IllegalStateException("Cannot inspect Ratchet AOT types under " + pattern, failure);
    }
  }

  private static void register(RuntimeHints hints, Class<?> type) {
    var binding = new BindingReflectionHintsRegistrar();
    binding.registerReflectionHints(hints.reflection(), type);
    for (Class<?> current = type;
        current != null && current != Object.class;
        current = current.getSuperclass()) {
      hints
          .reflection()
          .registerType(
              current,
              MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
              MemberCategory.INVOKE_DECLARED_METHODS,
              MemberCategory.DECLARED_FIELDS);
      for (Method method : current.getDeclaredMethods()) {
        binding.registerReflectionHints(hints.reflection(), method.getGenericReturnType());
        binding.registerReflectionHints(hints.reflection(), method.getGenericParameterTypes());
      }
      for (Class<?> contract : current.getInterfaces()) {
        hints.reflection().registerType(contract, MemberCategory.INVOKE_DECLARED_METHODS);
      }
    }
    hints.resources().registerPattern(type.getName().replace('.', '/') + ".class");
  }
}
