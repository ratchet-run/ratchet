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
package run.ratchet.quarkus.deployment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.deployment.builditem.ApplicationIndexBuildItem;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.nativeimage.LambdaCapturingTypeBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Provider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.jboss.jandex.CompositeIndex;
import org.jboss.jandex.Index;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.quarkus.runtime.RegisterJobSubmitter;

class RatchetProcessorTest {
  @Test
  void applicationSubmittersHaveBothKindsOfMetadataRegardlessOfInjectionShape() throws IOException {
    class LocalSubmitter {}
    Object anonymous = new Object() {};
    Class<?>[] applicationClasses = {
      LookupSubmitter.class,
      WrappedSubmitter.class,
      InheritedSubmitter.class,
      OuterSubmitter.NestedSubmitter.class,
      LocalSubmitter.class,
      anonymous.getClass()
    };
    var metadata = register(Index.of(applicationClasses), Index.of(DirectSubmitter.class));
    for (Class<?> applicationClass : applicationClasses) {
      assertTrue(metadata.lambdas.contains(applicationClass.getName()), applicationClass.getName());
      assertTrue(
          metadata.resources.contains(resource(applicationClass)), applicationClass.getName());
    }
  }

  @Test
  void dependencyDiscoveryAndExplicitOptInArePreserved() throws IOException {
    var metadata =
        register(
            Index.of(LookupSubmitter.class),
            Index.of(DirectSubmitter.class, AnnotatedSubmitter.class, UnrelatedDependency.class));
    assertTrue(metadata.lambdas.contains(DirectSubmitter.class.getName()));
    assertTrue(metadata.lambdas.contains(AnnotatedSubmitter.class.getName()));
    assertFalse(metadata.lambdas.contains(UnrelatedDependency.class.getName()));
    assertEquals(
        metadata.lambdas.stream()
            .map(n -> n.replace('.', '/') + ".class")
            .collect(Collectors.toSet()),
        Set.copyOf(metadata.resources));
  }

  @Test
  void wrappedDependencyInjectionIsDetected() throws IOException {
    var metadata =
        register(
            Index.of(new Class<?>[0]), Index.of(WrappedSubmitter.class, ProviderSubmitter.class));
    assertEquals(
        Set.of(WrappedSubmitter.class.getName(), ProviderSubmitter.class.getName()),
        Set.copyOf(metadata.lambdas));
  }

  @Test
  void overlappingIndexesDoNotDuplicateRegistration() throws IOException {
    var metadata = register(Index.of(DirectSubmitter.class), Index.of(DirectSubmitter.class));
    assertEquals(List.of(DirectSubmitter.class.getName()), metadata.lambdas);
    assertEquals(List.of(resource(DirectSubmitter.class)), metadata.resources);
  }

  private static Metadata register(Index application, Index dependency) {
    var combined = CompositeIndex.create(application, dependency);
    var item = new CombinedIndexBuildItem(combined, combined);
    List<LambdaCapturingTypeBuildItem> lambdas = new ArrayList<>();
    List<NativeImageResourceBuildItem> resources = new ArrayList<>();
    var processor = new RatchetProcessor();
    processor.lambdaCapturingTypes(
        new ApplicationIndexBuildItem(application), item, lambdas::add, resources::add);
    return new Metadata(
        lambdas.stream().map(LambdaCapturingTypeBuildItem::getClassName).toList(),
        resources.stream().flatMap(r -> r.getResources().stream()).toList());
  }

  private static String resource(Class<?> type) {
    return type.getName().replace('.', '/') + ".class";
  }

  private record Metadata(List<String> lambdas, List<String> resources) {}

  static class LookupSubmitter {}

  static class DirectSubmitter {
    JobSchedulerService scheduler;
  }

  static class WrappedSubmitter {
    Instance<JobSchedulerService> scheduler;
  }

  static class ProviderSubmitter {
    ProviderSubmitter(Provider<JobSchedulerService> scheduler) {}
  }

  static class InheritedSubmitter extends DirectSubmitter {}

  static class OuterSubmitter {
    static class NestedSubmitter {}
  }

  @RegisterJobSubmitter
  static class AnnotatedSubmitter {}

  static class UnrelatedDependency {}
}
