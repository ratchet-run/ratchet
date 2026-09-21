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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import example.aot.jobs.ApplicationTypes;
import example.aot.library.LazyJob;
import example.aot.library.LibraryTypes;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aot.generate.GeneratedFiles.Kind;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;

class RatchetApplicationAotProcessorTest {
  @Test
  void includesLexicalClassesAndPayloadsWithoutInitializingOrConstructingJobs() throws Exception {
    var factory = factory();
    var lazy = new RootBeanDefinition(LazyJob.class);
    lazy.setLazyInit(true);
    factory.registerBeanDefinition("lazy", lazy);
    var prototype = new RootBeanDefinition(LazyJob.class);
    prototype.setScope("prototype");
    factory.registerBeanDefinition("prototype", prototype);
    var types = RatchetApplicationAotProcessor.discover(factory);
    assertThat(types.keySet())
        .contains(
            ApplicationTypes.class.getName(),
            ApplicationTypes.Nested.class.getName(),
            ApplicationTypes.Payload.class.getName(),
            "example.aot.jobs.ApplicationTypes$1Local",
            "example.aot.jobs.ApplicationTypes$1",
            LazyJob.class.getName());
    assertThat(types.keySet()).doesNotContain(LibraryTypes.class.getName());
    var hints = new RuntimeHints();
    var files = generate(factory, hints);
    assertThat(
            RuntimeHintsPredicates.reflection()
                .onConstructor(ApplicationTypes.Payload.class.getDeclaredConstructor(String.class))
                .invoke())
        .accepts(hints);
    assertThat(
            RuntimeHintsPredicates.resource()
                .forResource("example/aot/jobs/ApplicationTypes$1.class"))
        .accepts(hints);
    String metadata =
        files.getGeneratedFileContent(
            Kind.RESOURCE, "META-INF/native-image/run.ratchet/spring/serialization-config.json");
    assertThat(metadata).contains("example.aot.jobs.ApplicationTypes$1");
    assertThat(metadata)
        .isEqualTo(
            generate(factory, new RuntimeHints())
                .getGeneratedFileContent(
                    Kind.RESOURCE,
                    "META-INF/native-image/run.ratchet/spring/serialization-config.json"));
    assertThat(factory.containsSingleton("lazy")).isFalse();
    assertThat(factory.containsSingleton("prototype")).isFalse();
  }

  @Test
  void dependencyRegistrationIncludesNestedTypesButDoesNotChangeClassPolicy() {
    var factory = factory();
    factory.registerBeanDefinition("registration", new RootBeanDefinition(Explicit.class));
    var policy =
        new run.ratchet.ri.security.PackagePrefixClassPolicy(java.util.Set.of("example.aot.jobs"));
    factory.registerSingleton("classPolicy", policy);
    assertThat(RatchetApplicationAotProcessor.discover(factory))
        .containsKeys(LibraryTypes.class.getName(), LibraryTypes.Nested.class.getName());
    assertThat(factory.getSingleton("classPolicy")).isSameAs(policy);
    assertThat(policy.isAllowed(LibraryTypes.class.getName())).isFalse();
  }

  @Test
  void packageMarkerIncludesDependencyPayloads() {
    var factory = factory();
    factory.registerBeanDefinition("registration", new RootBeanDefinition(Packages.class));
    assertThat(RatchetApplicationAotProcessor.discover(factory))
        .containsKeys(LibraryTypes.Payload.class.getName());
  }

  @Test
  void disabledConfigurationContributesNothing() {
    assertThat(
            new RatchetApplicationAotProcessor()
                .processAheadOfTime(new DefaultListableBeanFactory()))
        .isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"method", "field", "generic", "superclass"})
  void unavailableOptionalTypesAreSkippedWithoutDroppingHealthyJobs(
      String shape, @TempDir Path directory) throws Exception {
    try (var loader = optionalTypes(shape, directory)) {
      var factory = factory();
      factory.setBeanClassLoader(loader);
      AutoConfigurationPackages.register(factory, "example.optional.application");
      var present = new RuntimeHints();
      generate(factory, present);
      assertThat(
              present
                  .reflection()
                  .getTypeHint(
                      org.springframework.aot.hint.TypeReference.of(
                          "example.optional.application.OptionalJobs")))
          .isNotNull();
    }
    Files.delete(directory.resolve("example/optional/dependency/Dependency.class"));
    try (var loader =
        new URLClassLoader(new URL[] {directory.toUri().toURL()}, getClass().getClassLoader())) {
      var factory = factory();
      factory.setBeanClassLoader(loader);
      AutoConfigurationPackages.register(factory, "example.optional.application");
      var hints = new RuntimeHints();
      generate(factory, hints);
      assertThat(
              hints
                  .reflection()
                  .getTypeHint(
                      org.springframework.aot.hint.TypeReference.of(
                          "example.optional.application.OptionalJobs")))
          .isNull();
      assertThat(
              hints
                  .reflection()
                  .getTypeHint(
                      org.springframework.aot.hint.TypeReference.of(
                          "example.optional.application.HealthyJob")))
          .isNotNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unavailableRequiredTypeStillFailsAot(boolean explicit, @TempDir Path directory)
      throws Exception {
    try (var ignored = optionalTypes("method", directory)) {
      Files.delete(directory.resolve("example/optional/dependency/Dependency.class"));
    }
    try (var loader =
        new URLClassLoader(new URL[] {directory.toUri().toURL()}, getClass().getClassLoader())) {
      var factory = factory();
      factory.setBeanClassLoader(loader);
      factory.registerBeanDefinition(
          "requiredJob",
          new RootBeanDefinition(
              Class.forName(
                  explicit
                      ? "example.optional.registration.Registration"
                      : "example.optional.application.OptionalJobs",
                  false,
                  loader)));
      assertThatThrownBy(() -> generate(factory, new RuntimeHints()))
          .isInstanceOf(NoClassDefFoundError.class)
          .hasMessageContaining("Dependency");
    }
  }

  private URLClassLoader optionalTypes(String shape, Path directory) throws Exception {
    String members =
        switch (shape) {
          case "method" -> "public Dependency run() { return null; }";
          case "field" -> "public Dependency value;";
          case "generic" -> "public java.util.List<Dependency> run() { return null; }";
          default -> "";
        };
    Path dependency = directory.resolve("Dependency.java");
    Path optional = directory.resolve("OptionalJobs.java");
    Path healthy = directory.resolve("HealthyJob.java");
    Path registration = directory.resolve("Registration.java");
    Files.writeString(
        registration,
        "package example.optional.registration; @run.ratchet.spring.boot.autoconfigure.RegisterRatchetTypes(example.optional.application.OptionalJobs.class) public class Registration {}");
    Files.writeString(
        dependency, "package example.optional.dependency; public class Dependency {}");
    Files.writeString(
        optional,
        """
        package example.optional.application;
        import example.optional.dependency.Dependency;
        @org.springframework.context.annotation.Configuration(proxyBeanMethods=false)
        @org.springframework.boot.autoconfigure.condition.ConditionalOnClass(name="example.optional.dependency.Dependency")
        public class OptionalJobs %s { %s }
        """
            .formatted(shape.equals("superclass") ? "extends Dependency" : "", members));
    Files.writeString(
        healthy,
        """
        package example.optional.application;
        public class HealthyJob {
          public HealthyJob() { throw new AssertionError("AOT constructed a job"); }
          public void run() {}
        }
        """);
    assertThat(
            ToolProvider.getSystemJavaCompiler()
                .run(
                    null,
                    null,
                    null,
                    "--release",
                    "17",
                    "-classpath",
                    System.getProperty("java.class.path"),
                    "-d",
                    directory.toString(),
                    dependency.toString(),
                    optional.toString(),
                    healthy.toString(),
                    registration.toString()))
        .isZero();
    return new URLClassLoader(new URL[] {directory.toUri().toURL()}, getClass().getClassLoader());
  }

  private static DefaultListableBeanFactory factory() {
    var factory = new DefaultListableBeanFactory();
    factory.registerBeanDefinition(
        "ratchet", new RootBeanDefinition(RatchetAutoConfiguration.class));
    AutoConfigurationPackages.register(factory, "example.aot.jobs");
    return factory;
  }

  private static InMemoryGeneratedFiles generate(
      DefaultListableBeanFactory factory, RuntimeHints hints) {
    var context = mock(GenerationContext.class);
    var files = new InMemoryGeneratedFiles();
    when(context.getGeneratedFiles()).thenReturn(files);
    when(context.getRuntimeHints()).thenReturn(hints);
    new RatchetApplicationAotProcessor().processAheadOfTime(factory).applyTo(context, null);
    return files;
  }

  @RegisterRatchetTypes(LibraryTypes.class)
  static class Explicit {}

  @RegisterRatchetTypes(basePackageClasses = LibraryTypes.class)
  static class Packages {}
}
