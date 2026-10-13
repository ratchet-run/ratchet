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
package run.ratchet.micronaut;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.*;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import io.micronaut.transaction.TransactionOperations;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.DefaultJobCreationService;
import run.ratchet.ri.core.DefaultJobSchedulerService;
import run.ratchet.ri.core.internal.DefaultRecurringAnnotationMaintenanceService;
import run.ratchet.spi.ExecutorProvider;

@AnalyzeClasses(
    packages = "run.ratchet.micronaut",
    importOptions = ImportOption.DoNotIncludeTests.class)
class MicronautIntegrationArchitectureTest {
  @ArchTest
  static final ArchRule noCdiOrSpring =
      noClasses()
          .that()
          .resideInAPackage("run.ratchet.micronaut..")
          .and()
          .haveNameNotMatching(".*\\.\\$.*")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("jakarta.enterprise..", "org.springframework..");

  @ArchTest
  static final ArchRule persistenceIndependent =
      noClasses()
          .that()
          .resideInAPackage("run.ratchet.micronaut..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "jakarta.persistence..",
              "org.hibernate..",
              "run.ratchet.store.mysql..",
              "run.ratchet.store.postgresql..",
              "run.ratchet.store.mongodb..",
              "run.ratchet.micronaut.jpa..",
              "run.ratchet.micronaut.mongodb..",
              "com.mongodb..");

  // Concrete products are needed for disposal and for the no-transaction adapter's API;
  // their conditions deliberately cover the broader application SPI instead.
  private static final Map<String, Class<?>> MISSING_BEAN_TYPE_EXCEPTIONS =
      Map.of(
          "RatchetRuntimeConfiguration.ratchetExecutorProvider", ExecutorProvider.class,
          "RatchetAutoConfiguration.ratchetNoTransactionOperations", TransactionOperations.class);

  @ArchTest
  static void everyNonProxiedFactoryProductRequiresMissingBean(JavaClasses classes)
      throws ClassNotFoundException {
    Set<String> usedExceptions = new HashSet<>();
    for (var imported : classes) {
      if (!imported.isAnnotatedWith(Factory.class)
          || !imported.getName().contains("Ratchet")
          || !imported.getName().contains("Configuration")
          || imported.getName().contains(".$")) continue;
      Class<?> factory =
          Class.forName(
              imported.getName(),
              false,
              MicronautIntegrationArchitectureTest.class.getClassLoader());
      for (var method : factory.getDeclaredMethods()) {
        if (!method.isAnnotationPresent(Singleton.class)
            && !method.isAnnotationPresent(Context.class)
            && !method.isAnnotationPresent(Bean.class)) continue;
        boolean proxied =
            method.isAnnotationPresent(RatchetTransactionBoundary.class)
                || method.isAnnotationPresent(Transactional.class);
        var missing =
            Arrays.stream(method.getAnnotationsByType(Requires.class))
                .flatMap(requirement -> Arrays.stream(requirement.missingBeans()))
                .toList();
        if (proxied) {
          assertTrue(missing.isEmpty(), method.toString());
          assertTrue(method.isAnnotationPresent(Secondary.class), method.toString());
          continue;
        }
        String key = factory.getSimpleName() + "." + method.getName();
        Class<?> expected = MISSING_BEAN_TYPE_EXCEPTIONS.getOrDefault(key, method.getReturnType());
        if (MISSING_BEAN_TYPE_EXCEPTIONS.containsKey(key)) usedExceptions.add(key);
        assertEquals(List.of(expected), missing, method.toString());
        assertTrue(expected.isAssignableFrom(method.getReturnType()), method.toString());
        assertFalse(method.isAnnotationPresent(Secondary.class), method.toString());
      }
    }
    assertEquals(MISSING_BEAN_TYPE_EXCEPTIONS.keySet(), usedExceptions);
  }

  @ArchTest
  static void eachMissingBeanTypeHasOnlyOneDefaultFactoryMethod(JavaClasses classes)
      throws ClassNotFoundException {
    Map<Class<?>, String> defaults = new HashMap<>();
    for (var imported : classes) {
      if (!imported.isAnnotatedWith(Factory.class)
          || !imported.getName().contains("Ratchet")
          || !imported.getName().contains("Configuration")
          || imported.getName().contains(".$")) continue;
      Class<?> factory =
          Class.forName(
              imported.getName(),
              false,
              MicronautIntegrationArchitectureTest.class.getClassLoader());
      for (var method : factory.getDeclaredMethods()) {
        if (!method.isAnnotationPresent(Singleton.class)
            && !method.isAnnotationPresent(Context.class)
            && !method.isAnnotationPresent(Bean.class)) continue;
        for (var requirement : method.getAnnotationsByType(Requires.class)) {
          for (Class<?> type : requirement.missingBeans()) {
            String previous = defaults.putIfAbsent(type, method.toString());
            assertTrue(
                previous == null || previous.equals(method.toString()),
                () -> type.getName() + " has multiple defaults: " + previous + " and " + method);
          }
        }
      }
    }
    assertFalse(defaults.isEmpty());
  }

  @ArchTest
  static void inspectsFactoriesAndAdapters(JavaClasses classes) {
    for (Class<?> type :
        new Class<?>[] {
          RatchetAutoConfiguration.class,
          RatchetEngineAutoConfiguration.class,
          RatchetExecutionConfiguration.class,
          RatchetRuntimeConfiguration.class,
          RatchetMicrometerAutoConfiguration.class,
          MicronautAfterCommitRegistrar.class,
          MicronautBeanResolver.class
        })
      assertTrue(classes.stream().anyMatch(imported -> imported.getName().equals(type.getName())));
  }

  @ArchTest
  static void factoryTransactionMarkerMatchesTransactionShape(JavaClasses classes) {
    Set<Class<?>> methodOnly =
        Set.of(
            BatchCompletionTransaction.class,
            DefaultJobCreationService.class,
            DefaultJobSchedulerService.class,
            DefaultRecurringAnnotationMaintenanceService.class);
    for (Class<?> factory :
        new Class<?>[] {
          RatchetSubmissionConfiguration.class,
          RatchetExecutionConfiguration.class,
          RatchetRuntimeConfiguration.class
        }) {
      for (var method : factory.getDeclaredMethods()) {
        Transactional classLevel = method.getReturnType().getAnnotation(Transactional.class);
        boolean methodLevel =
            methodOnly.stream().anyMatch(type -> type.isAssignableFrom(method.getReturnType()));
        // Class-level shapes carry the marker plus a copy of the class default (the marker alone
        // drops it); method-only shapes carry the marker alone.
        assertEquals(
            classLevel != null || methodLevel,
            method.isAnnotationPresent(RatchetTransactionBoundary.class),
            method.toString());
        assertEquals(classLevel, method.getAnnotation(Transactional.class), method.toString());
      }
    }
  }

  @ArchTest
  static void allClassLevelTransactionalServicesAreFactoryProducts(JavaClasses classes) {
    Set<String> exposed = new HashSet<>();
    for (Class<?> factory :
        new Class<?>[] {
          RatchetSubmissionConfiguration.class,
          RatchetExecutionConfiguration.class,
          RatchetRuntimeConfiguration.class
        }) {
      for (var method : factory.getDeclaredMethods()) {
        for (Class<?> type = method.getReturnType(); type != null; type = type.getSuperclass()) {
          if (type.isAnnotationPresent(Transactional.class)
              && type.getPackageName().startsWith("run.ratchet.ri."))
            exposed.add(type.getSimpleName());
        }
      }
    }
    assertEquals(
        Set.of(
            "BatchService",
            "ChainScheduler",
            "DeadLetterService",
            "DefaultJobArchivingService",
            "JobCascadeService",
            "JobStateManager",
            "PollerCycleExecutor",
            "PostExecutionHandler",
            "RecurringJobExecutor",
            "WorkflowScheduler"),
        exposed);
  }
}
