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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Instance;
import jakarta.transaction.Transactional;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.cdi.CdiBeanResolver;
import run.ratchet.ri.core.JobStateManager;

class RatchetRuntimeComponentCatalogTest {

  private static final List<String> EXPECTED_COMPONENT_ORDER =
      List.of(
          "JakartaAfterCommitRegistrar",
          "CdiBeanResolver",
          "RecurringMethodInvoker",
          "InternalEventPublisher",
          "CallerPrincipalProvider",
          "JobPayloadInputValidator",
          "JobSecurityValidator",
          "DoNotRetryPolicy",
          "PreExecutionValidator",
          "DefaultDrainController",
          "JobTypeRateLimiter",
          "PoolRegistry",
          "DynamicHeartbeatCalculator",
          "DefaultNodeIdentityProvider",
          "SingletonLeaseService",
          "StoreBackedStartupCoordinator",
          "RecurringRegistrationState",
          "JobPayloadInvoker",
          "ExecutionObserver",
          "JobSuccessFinalizer",
          "DefaultJobLoggerFactory",
          "DefaultResultPersistenceStrategy",
          "ExecutionTargetRouter",
          "WorkflowConditionEvaluator",
          "ChainScheduler",
          "WorkflowScheduler",
          "RecurringJobExecutor",
          "DefaultRecurringScheduler",
          "JobWakeupService",
          "DefaultResourcePermitService",
          "BatchRecoveryService",
          "DeadLetterService",
          "BatchService",
          "BatchRecoveryTimer",
          "DefaultJobArchivingService",
          "LogPurgeTimer",
          "PostExecutionHandler",
          "JobStateManager",
          "RetryBufferManager",
          "SubmissionGateChecker",
          "SubmissionFailureHandler",
          "DefaultJobExecutorService",
          "JobSubmissionService",
          "RetryBufferDrainer",
          "JobExecutionCoordinator",
          "JobTimeoutHandler",
          "Poller",
          "PollerCycleExecutor",
          "DefaultPollerScheduler",
          "PollerWakeupListener",
          "OrphanRecoveryTimer",
          "JobCascadeService",
          "DefaultJobCreationService",
          "DefaultInvocationSubmissionService",
          "DefaultJobSchedulerService",
          "DefaultJobQueryService",
          "DefaultClusterQueryService",
          "RecurringMethodRegistrar",
          "DefaultRatchetRuntime");

  @Test
  void catalogMatchesTransactionalSourceAndSelectedConstructors() throws Exception {
    List<RatchetComponentDescriptor> descriptors = RatchetRuntimeComponentCatalog.components();
    Set<Class<?>> transactionalInventory = transactionalInventory();

    assertEquals(
        EXPECTED_COMPONENT_ORDER,
        descriptors.stream()
            .map(RatchetComponentDescriptor::componentType)
            .map(Class::getSimpleName)
            .toList());

    for (RatchetComponentDescriptor descriptor : descriptors) {
      Class<?> componentType = descriptor.componentType();
      assertEquals(
          transactionalInventory.contains(componentType),
          descriptor.transactional(),
          () -> componentType.getName() + " has incorrect transactional catalog metadata");
      assertTrue(descriptor.singletonScope());
      if (descriptor.transactional()) {
        assertTrue(
            Modifier.isPublic(componentType.getModifiers()),
            () -> componentType.getName() + " must be public for class-based transaction proxies");
        assertFalse(
            Modifier.isFinal(componentType.getModifiers()),
            () ->
                componentType.getName() + " must not be final for class-based transaction proxies");
      }

      Class<?>[] parameterTypes = descriptor.constructorParameterTypes().toArray(Class<?>[]::new);
      Constructor<?> constructor = componentType.getDeclaredConstructor(parameterTypes);
      assertTrue(constructor.trySetAccessible());
      assertArrayEquals(parameterTypes, constructor.getParameterTypes());
      if (componentType != CdiBeanResolver.class) {
        assertFalse(
            descriptor.constructorParameterTypes().contains(Instance.class),
            () -> componentType.getName() + " selects a CDI Instance constructor");
      }
      assertFalse(
          descriptor.constructorParameterTypes().contains(Event.class),
          () -> componentType.getName() + " selects a CDI Event constructor");
      assertTrue(
          descriptor.constructorParameterTypes().stream().noneMatch(Class::isPrimitive),
          () -> componentType.getName() + " selects a primitive configuration argument");
    }
  }

  @Test
  void catalogClosesEveryRiManagedConstructorDependency() throws Exception {
    List<RatchetComponentDescriptor> descriptors = RatchetRuntimeComponentCatalog.components();
    Set<Class<?>> componentTypes =
        descriptors.stream()
            .map(RatchetComponentDescriptor::componentType)
            .collect(Collectors.toSet());

    for (RatchetComponentDescriptor descriptor : descriptors) {
      Constructor<?> constructor =
          descriptor
              .componentType()
              .getDeclaredConstructor(
                  descriptor.constructorParameterTypes().toArray(Class<?>[]::new));
      for (Class<?> parameterType : constructor.getParameterTypes()) {
        if (!parameterType.getPackageName().startsWith("run.ratchet.ri")) {
          continue;
        }
        assertTrue(
            parameterType == RecurringMethodDiscovery.class
                || componentTypes.stream().anyMatch(parameterType::isAssignableFrom),
            () ->
                descriptor.componentType().getName()
                    + " has uncatalogued RI dependency "
                    + parameterType.getName());
      }
    }
  }

  private static Set<Class<?>> transactionalInventory()
      throws IOException, URISyntaxException, ClassNotFoundException {
    Set<Class<?>> inventory = new HashSet<>();
    for (Class<?> candidate : classesUnder("run.ratchet.ri.core")) {
      if (isTransactional(candidate)) {
        inventory.add(candidate);
      }
    }
    return inventory;
  }

  private static boolean isTransactional(Class<?> type) {
    return type.isAnnotationPresent(Transactional.class)
        || Arrays.stream(type.getDeclaredMethods())
            .anyMatch(method -> method.isAnnotationPresent(Transactional.class));
  }

  private static Set<Class<?>> classesUnder(String packageName)
      throws IOException, URISyntaxException, ClassNotFoundException {
    Path artifact =
        Path.of(JobStateManager.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    String packagePath = packageName.replace('.', '/');
    Set<String> classNames = new HashSet<>();

    if (Files.isDirectory(artifact)) {
      Path packageDirectory = artifact.resolve(packagePath);
      try (Stream<Path> paths = Files.walk(packageDirectory)) {
        paths
            .filter(Files::isRegularFile)
            .map(packageDirectory::relativize)
            .map(Path::toString)
            .filter(name -> name.endsWith(".class"))
            .map(name -> packageName + "." + toClassName(name))
            .forEach(classNames::add);
      }
    } else {
      try (JarFile jar = new JarFile(artifact.toFile())) {
        jar.stream()
            .map(entry -> entry.getName())
            .filter(name -> name.startsWith(packagePath + "/"))
            .filter(name -> name.endsWith(".class"))
            .map(RatchetRuntimeComponentCatalogTest::toClassName)
            .forEach(classNames::add);
      }
    }

    ClassLoader classLoader = JobStateManager.class.getClassLoader();
    Set<Class<?>> classes = new HashSet<>();
    for (String className : classNames) {
      if (!className.endsWith("module-info") && !className.endsWith("package-info")) {
        classes.add(Class.forName(className, false, classLoader));
      }
    }
    return classes;
  }

  private static String toClassName(String classFileName) {
    return classFileName
        .substring(0, classFileName.length() - ".class".length())
        .replace('/', '.')
        .replace('\\', '.');
  }
}
