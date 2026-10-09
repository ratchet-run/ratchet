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
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ApplicationDiscoveryTest {
  @Test
  void methodReferencesDiscoverTargetAndPojoArgument() throws Exception {
    var discovery = discovery(DiscoveryFixtures.MethodReferenceSubmitter.class);
    assertTrue(discovery.targets().contains(DiscoveryFixtures.Target.class.getName()));
    assertTrue(discovery.bindingTypes().contains(DiscoveryFixtures.Pojo.class.getName()));
    assertTrue(discovery.lambdaCapturing());
    assertFalse(ApplicationDiscovery.referencesRatchet(bytes(DiscoveryFixtures.Target.class)));
    assertTrue(
        ApplicationDiscovery.discoverClasses(
                classes(
                    DiscoveryFixtures.MethodReferenceSubmitter.class,
                    DiscoveryFixtures.Target.class))
            .contains(DiscoveryFixtures.Target.class.getName()));
  }

  @Test
  void capturingLambdaDiscoversRecordArrayComponentAndReturnType() throws Exception {
    var discovery = discovery(DiscoveryFixtures.CapturingSubmitter.class);
    assertTrue(discovery.targets().contains(DiscoveryFixtures.Target.class.getName()));
    assertTrue(discovery.bindingTypes().contains(DiscoveryFixtures.Order.class.getName()));
    assertTrue(discovery.lambdaCapturing());
  }

  @Test
  void syntheticLambdaBodyDiscoversStaticTarget() throws Exception {
    var discovery = discovery(DiscoveryFixtures.StaticSubmitter.class);
    assertTrue(discovery.targets().contains(DiscoveryFixtures.Target.class.getName()));
    assertFalse(discovery.bindingTypes().contains(String.class.getName()));
  }

  @Test
  void superclassSubmissionIncludesSubclassAndImplementedInterface() throws Exception {
    var classes =
        classes(
            DiscoveryFixtures.ParentSubmitter.class,
            DiscoveryFixtures.InheritedSubmitter.class,
            DiscoveryFixtures.Target.class,
            DiscoveryFixtures.TargetContract.class);
    assertFalse(
        ApplicationDiscovery.referencesRatchet(bytes(DiscoveryFixtures.InheritedSubmitter.class)));
    var discovered = ApplicationDiscovery.discoverClasses(classes);
    assertTrue(discovered.contains(DiscoveryFixtures.InheritedSubmitter.class.getName()));
    assertTrue(discovered.contains(DiscoveryFixtures.TargetContract.class.getName()));
  }

  @Test
  void enclosingSubmitterIncludesNestedAnonymousAndLocalClasses() throws Exception {
    var enclosing = DiscoveryFixtures.EnclosingSubmitter.class;
    var instance = new DiscoveryFixtures.EnclosingSubmitter();
    Class<?> local = instance.local(new DiscoveryFixtures.Target()).getClass();
    Class<?> anonymous = instance.anonymous(new DiscoveryFixtures.Target()).getClass();
    var discovered =
        ApplicationDiscovery.discoverClasses(
            classes(
                enclosing,
                DiscoveryFixtures.EnclosingSubmitter.Nested.class,
                DiscoveryFixtures.EnclosingSubmitter.NonSubmittingNested.class,
                local,
                anonymous,
                DiscoveryFixtures.Target.class));
    assertTrue(discovered.contains(DiscoveryFixtures.EnclosingSubmitter.Nested.class.getName()));
    assertTrue(
        discovered.contains(
            DiscoveryFixtures.EnclosingSubmitter.NonSubmittingNested.class.getName()));
    assertFalse(
        ApplicationDiscovery.referencesRatchet(
            bytes(DiscoveryFixtures.EnclosingSubmitter.NonSubmittingNested.class)));
    assertTrue(discovered.contains(local.getName()));
    assertTrue(discovered.contains(anonymous.getName()));
    assertTrue(discovery(local).lambdaCapturing());
    assertTrue(discovery(anonymous).lambdaCapturing());
  }

  @Test
  void recurringBeanIsItsOwnTargetAndUnrelatedClassIsSkipped() throws Exception {
    assertEquals(
        Set.of(DiscoveryFixtures.RecurringBean.class.getName()),
        discovery(DiscoveryFixtures.RecurringBean.class).targets());
    assertFalse(ApplicationDiscovery.referencesRatchet(bytes(DiscoveryFixtures.Unrelated.class)));
    assertTrue(
        ApplicationDiscovery.discoverClasses(classes(DiscoveryFixtures.Unrelated.class)).isEmpty());
  }

  @Test
  void frameworkAndMicronautGeneratedClassesAreExcludedByPackageBoundary() {
    for (String name :
        Set.of(
            "java.lang.String",
            "jakarta.persistence.EntityManager",
            "io.micronaut.context.BeanContext",
            "run.ratchet.api.JobBuilder",
            "run.ratchet.ri.core.Job",
            "example.Jobs$Definition",
            "example.Jobs$Introspection",
            "example.Jobs$Exec",
            "example.Jobs$Reference",
            "example.Jobs$Intercepted")) {
      assertFalse(ApplicationDiscovery.isApplicationClass(name), name);
    }
    assertTrue(ApplicationDiscovery.isApplicationClass("example.Jobs"));
    assertTrue(ApplicationDiscovery.isApplicationClass("javaapplication.Jobs"));
  }

  private static ApplicationDiscovery.Discovery discovery(Class<?> type) throws IOException {
    return ApplicationDiscovery.discover(bytes(type));
  }

  private static Map<String, byte[]> classes(Class<?>... types) throws IOException {
    var classes = new LinkedHashMap<String, byte[]>();
    for (Class<?> type : types) classes.put(type.getName(), bytes(type));
    return classes;
  }

  private static byte[] bytes(Class<?> type) throws IOException {
    try (var input = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
      assertNotNull(input);
      return input.readAllBytes();
    }
  }
}
