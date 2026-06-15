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
package run.ratchet.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Module-hygiene guardrails that hold against today's tree. Each rule locks in an invariant the
 * Jakarta EE spec-candidacy posture depends on, so the first PR that would silently break it fails
 * here instead of deep in the server matrix.
 *
 * <ul>
 *   <li>Observability providers stay out of the core (api/spi/ri/stores/coordinators).
 *   <li>The JPA API is confined to the store layer.
 *   <li>The serializable-lambda deserialization surface stays in {@code ratchet-api}.
 *   <li>Implementation modules never print to stdout/stderr or call {@code printStackTrace}.
 *   <li>JPA entities are non-final and carry a non-private no-arg constructor.
 *   <li>SQL store dialects are mutually isolated.
 *   <li>{@code ratchet-api} does not import {@code jakarta.transaction}.
 *   <li>Coordinator modules depend only on {@code coordinator-common}, not on sibling coordinators.
 * </ul>
 */
@AnalyzeClasses(packages = "run.ratchet", importOptions = ImportOption.DoNotIncludeTests.class)
public class ModuleHygieneArchitectureTest {

  private static final String API = "run.ratchet.api..";
  private static final String SPI = "run.ratchet.spi..";
  private static final String STORE_CORE = "run.ratchet.store..";
  private static final String RI = "run.ratchet.ri..";
  private static final String COORDINATOR = "run.ratchet.coordinator..";
  private static final String COORDINATOR_JMS = "run.ratchet.coordinator.jms..";
  private static final String COORDINATOR_POSTGRESQL = "run.ratchet.coordinator.postgresql..";
  private static final String COORDINATOR_INFINISPAN = "run.ratchet.coordinator.infinispan..";
  private static final String COORDINATOR_HAZELCAST = "run.ratchet.coordinator.hazelcast..";

  /**
   * Defends against a vacuous pass: if the importer returns no production classes (a known
   * surefire/JaCoCo parallel-fork interaction) every rule below matches zero subjects and passes.
   */
  @Test
  void archunitImporterSeesAllModules() {
    JavaClasses imported =
        new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("run.ratchet");
    assertTrue(imported.size() > 50, "ArchUnit importer loaded only " + imported.size());
    assertPackageNonEmpty(imported, "run.ratchet.api");
    assertPackageNonEmpty(imported, "run.ratchet.spi");
    assertPackageNonEmpty(imported, "run.ratchet.store");
    assertPackageNonEmpty(imported, "run.ratchet.store.mysql");
    assertPackageNonEmpty(imported, "run.ratchet.store.postgresql");
    assertPackageNonEmpty(imported, "run.ratchet.store.mongodb");
    assertPackageNonEmpty(imported, "run.ratchet.ri");
    assertPackageNonEmpty(imported, "run.ratchet.coordinator");
    assertPackageNonEmpty(imported, "run.ratchet.coordinator.jms");
    assertPackageNonEmpty(imported, "run.ratchet.coordinator.postgresql");
    assertPackageNonEmpty(imported, "run.ratchet.coordinator.infinispan");
    assertPackageNonEmpty(imported, "run.ratchet.coordinator.hazelcast");
  }

  private static void assertPackageNonEmpty(JavaClasses imported, String packageName) {
    long count = imported.stream().filter(c -> c.getPackageName().startsWith(packageName)).count();
    assertTrue(count > 0, "expected at least one class in " + packageName + ", found 0");
  }

  // --- 1. Observability providers stay out of the core ---

  /**
   * The api/spi/ri/store/coordinator modules must not depend on a concrete observability provider.
   * Metrics and tracing are reached through the {@code MetricsCollector}/{@code TracingCollector}
   * SPIs; a direct Micrometer or OpenTelemetry import would hardwire the core to one provider and
   * break the dependency-free public surface. The provider adapters live in {@code
   * ratchet-micrometer}/{@code ratchet-otel}, which are outside these packages.
   */
  @ArchTest
  static final ArchRule coreModulesDoNotDependOnObservabilityProviders =
      noClasses()
          .that()
          .resideInAnyPackage(API, SPI, RI, STORE_CORE, COORDINATOR)
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("io.micrometer..", "io.opentelemetry..")
          .because(
              "metrics and tracing are reached through the MetricsCollector/TracingCollector SPIs; "
                  + "a direct provider import hardwires the core to one observability backend");

  // --- 2. JPA API confined to the store layer ---

  /**
   * The {@code jakarta.persistence} API stays in the store layer. The public API, the reference
   * implementation, and the coordinators are JPA-free — persistence is reached only through the
   * store SPI. Scoped to SQL-stores-only is unnecessary: store-core legitimately uses JPA, and the
   * Mongo store does not, so confining to {@code run.ratchet.store..} subsumes both.
   */
  @ArchTest
  static final ArchRule jpaApiIsConfinedToStoreModules =
      noClasses()
          .that()
          .resideInAnyPackage(API, SPI, RI, COORDINATOR)
          .should()
          .dependOnClassesThat()
          .resideInAPackage("jakarta.persistence..")
          .because(
              "JPA is a store-layer detail; the API, RI, and coordinators reach persistence only "
                  + "through the store SPI and must stay free of jakarta.persistence");

  // --- 3. Serializable-lambda types stay in ratchet-api ---

  /**
   * The audited serializable-lambda surface ({@code Serializable*} functional interfaces) stays in
   * {@code run.ratchet.api}. These types feed the {@code LambdaSerializer} deserialization path,
   * which applies a JDK {@code ObjectInputFilter}; keeping the surface in one package keeps the
   * audited set small and discoverable.
   */
  @ArchTest
  static final ArchRule serializableLambdaTypesStayInApi =
      classes()
          .that()
          .haveSimpleNameStartingWith("Serializable")
          .and()
          .resideInAPackage("run.ratchet..")
          .should()
          .resideInAPackage(API)
          .because(
              "the serializable-lambda functional interfaces are the audited deserialization "
                  + "surface; keeping them in ratchet-api keeps that surface in one place");

  // --- 4. No stdout/stderr or printStackTrace in implementation modules ---

  /**
   * Implementation modules (RI, store, coordinator) must not write to {@code System.out}/{@code
   * System.err} or call any {@code Throwable.printStackTrace} overload. Diagnostics go through
   * JBoss Logging so EE deployments route them consistently; a stray stack-trace dump bypasses the
   * configured logger.
   */
  @ArchTest
  static final ArchRule noStdoutStderrOrPrintStackTrace =
      noClasses()
          .that()
          .resideInAnyPackage(RI, STORE_CORE, COORDINATOR)
          .should()
          .accessField(System.class, "out")
          .orShould()
          .accessField(System.class, "err")
          .orShould()
          .callMethod(Throwable.class, "printStackTrace")
          .orShould()
          .callMethod(Throwable.class, "printStackTrace", java.io.PrintStream.class)
          .orShould()
          .callMethod(Throwable.class, "printStackTrace", java.io.PrintWriter.class)
          .because(
              "diagnostics go through JBoss Logging so EE deployments route them consistently; a "
                  + "stdout/stderr write or printStackTrace dump bypasses the configured logger");

  // --- 5. Entities are non-final with a non-private no-arg constructor ---

  /**
   * Every {@code @jakarta.persistence.Entity} must be non-final and declare a non-private no-arg
   * constructor. JPA providers instantiate managed entities reflectively and subclass them for lazy
   * proxies; a final entity or a private/absent no-arg constructor breaks one provider or another
   * in the matrix.
   */
  @ArchTest
  static final ArchRule entitiesAreNonFinalWithNonPrivateNoArgConstructor =
      classes()
          .that()
          .areAnnotatedWith(jakarta.persistence.Entity.class)
          .should()
          .notHaveModifier(JavaModifier.FINAL)
          .andShould(haveNonPrivateNoArgConstructor())
          .because(
              "JPA providers instantiate entities reflectively and subclass them for lazy proxies; "
                  + "a final entity or a private/absent no-arg constructor breaks one provider");

  // --- 6. SQL store dialects are mutually isolated ---

  /**
   * The SQL store dialect packages do not depend on each other. The MySQL store must not reach into
   * the PostgreSQL store or vice versa; each compiles against {@code ratchet-store-core} only.
   * Expressed as ArchUnit glob slices over {@code run.ratchet.store.(*)..} so the MongoDB store is
   * included for free.
   */
  @ArchTest
  static final ArchRule sqlStoreDialectsAreMutuallyIsolated =
      slices()
          .matching("run.ratchet.store.(mysql|postgresql|mongodb)..")
          .should()
          .notDependOnEachOther()
          .because(
              "each store dialect compiles against ratchet-store-core only; a cross-dialect "
                  + "dependency couples two stores that must ship independently");

  // --- 7. ratchet-api must not import jakarta.transaction ---

  /**
   * {@code ratchet-api} must not depend on {@code jakarta.transaction}. The transaction-attribute
   * contract is documented per-method in Javadoc and declared on the reference implementation,
   * never on the exported interface. The method-level {@code @Transactional} rule enforces only
   * half of this; the package-level import ban closes the gap.
   */
  @ArchTest
  static final ArchRule apiDoesNotDependOnJakartaTransaction =
      noClasses()
          .that()
          .resideInAnyPackage(API, SPI)
          .should()
          .dependOnClassesThat()
          .resideInAPackage("jakarta.transaction..")
          .because(
              "the transaction-attribute contract is documented per-method in Javadoc and declared "
                  + "on the reference implementation, never on the exported API");

  // --- 8. Coordinator mutual isolation ---

  /**
   * Each coordinator implementation depends only on {@code coordinator-common}, never on a sibling
   * coordinator. A JMS coordinator that reached into the PostgreSQL coordinator would couple two
   * independently-shipped pluggable modules.
   */
  @ArchTest
  static final ArchRule coordinatorsAreMutuallyIsolated =
      classes()
          .that()
          .resideInAnyPackage(
              COORDINATOR_JMS,
              COORDINATOR_POSTGRESQL,
              COORDINATOR_INFINISPAN,
              COORDINATOR_HAZELCAST)
          .should(notDependOnAnotherCoordinatorDialect())
          .because(
              "each coordinator implementation ships independently and depends only on "
                  + "coordinator-common; a sibling-coordinator dependency couples two pluggable "
                  + "modules");

  // --- 9. Coordinator threads go through the managed thread factory ---

  /**
   * No class in the RI or the coordinator layer may construct a raw {@link Thread} or {@link
   * java.util.concurrent.ThreadPoolExecutor}, or call a factory method on {@link
   * java.util.concurrent.Executors}. Threads must be obtained from the container's managed thread
   * factory so the runtime can govern them (Jakarta Concurrency).
   *
   * <p>Three resolver classes are exempt because they are the documented single source of threads:
   * {@code CoordinatorThreading} in coordinator-common, and {@code DefaultExecutorProvider} /
   * {@code StandaloneExecutorProvider} in the RI. {@code StandaloneExecutorProvider} and {@code
   * CoordinatorThreading}'s standalone path create raw daemon threads on purpose for plain-SE runs;
   * that is an explicit opt-in, never the container default.
   */
  @ArchTest
  static final ArchRule coordinatorsAndRiCreateNoRawThreadsOutsideResolvers =
      noClasses()
          .that()
          .resideInAnyPackage(RI, COORDINATOR)
          .and(areNotThreadResolvers())
          .should(createRawThreadsOrExecutors())
          .because(
              "threads must come from the container's managed thread factory so the runtime can "
                  + "govern them; only the documented resolver classes (CoordinatorThreading, "
                  + "DefaultExecutorProvider, StandaloneExecutorProvider) may create raw threads");

  // --- predicates and conditions ---

  private static final Set<String> THREAD_RESOLVERS =
      Set.of(
          "run.ratchet.coordinator.common.CoordinatorThreading",
          "run.ratchet.ri.cdi.internal.DefaultExecutorProvider",
          "run.ratchet.ri.cdi.StandaloneExecutorProvider");

  private static DescribedPredicate<JavaClass> areNotThreadResolvers() {
    return new DescribedPredicate<>("are not the documented thread-resolver classes") {
      @Override
      public boolean test(JavaClass clazz) {
        // Compare on the top-level class so a lambda/anonymous nested in a resolver is also exempt.
        String topLevel = clazz.getName().split("\\$", 2)[0];
        return !THREAD_RESOLVERS.contains(topLevel);
      }
    };
  }

  private static ArchCondition<JavaClass> createRawThreadsOrExecutors() {
    return new ArchCondition<>("construct a raw Thread/ThreadPoolExecutor or call Executors.new*") {
      @Override
      public void check(JavaClass clazz, ConditionEvents events) {
        clazz.getConstructorCallsFromSelf().stream()
            .filter(
                call -> {
                  String target = call.getTargetOwner().getFullName();
                  return target.equals("java.lang.Thread")
                      || target.equals("java.util.concurrent.ThreadPoolExecutor")
                      || target.equals("java.util.concurrent.ScheduledThreadPoolExecutor");
                })
            .forEach(
                call ->
                    events.add(
                        SimpleConditionEvent.violated(
                            clazz,
                            clazz.getFullName()
                                + " constructs a raw "
                                + call.getTargetOwner().getSimpleName()
                                + " at "
                                + call.getSourceCodeLocation())));
        clazz.getMethodCallsFromSelf().stream()
            .filter(
                call ->
                    call.getTargetOwner().getFullName().equals("java.util.concurrent.Executors"))
            .forEach(
                call ->
                    events.add(
                        SimpleConditionEvent.violated(
                            clazz,
                            clazz.getFullName()
                                + " calls Executors."
                                + call.getTarget().getName()
                                + " at "
                                + call.getSourceCodeLocation())));
      }
    };
  }

  private static ArchCondition<JavaClass> haveNonPrivateNoArgConstructor() {
    return new ArchCondition<>("have a non-private no-arg constructor") {
      @Override
      public void check(JavaClass clazz, ConditionEvents events) {
        boolean ok =
            clazz.getConstructors().stream()
                .anyMatch(
                    c ->
                        c.getRawParameterTypes().isEmpty()
                            && !c.getModifiers().contains(JavaModifier.PRIVATE));
        if (!ok) {
          events.add(
              SimpleConditionEvent.violated(
                  clazz,
                  clazz.getFullName()
                      + " is a JPA @Entity but has no non-private no-arg constructor; "
                      + "providers cannot instantiate it reflectively"));
        }
      }
    };
  }

  private static ArchCondition<JavaClass> notDependOnAnotherCoordinatorDialect() {
    return new ArchCondition<>("not depend on a sibling coordinator dialect") {
      @Override
      public void check(JavaClass clazz, ConditionEvents events) {
        String sourceDialect = coordinatorDialect(clazz.getPackageName());
        clazz.getDirectDependenciesFromSelf().stream()
            .map(dep -> coordinatorDialect(dep.getTargetClass().getPackageName()))
            .filter(targetDialect -> targetDialect != null && !targetDialect.equals(sourceDialect))
            .findFirst()
            .ifPresent(
                targetDialect ->
                    events.add(
                        SimpleConditionEvent.violated(
                            clazz,
                            clazz.getFullName()
                                + " (coordinator-"
                                + sourceDialect
                                + ") depends on coordinator-"
                                + targetDialect
                                + "; coordinators must depend only on coordinator-common")));
      }
    };
  }

  private static String coordinatorDialect(String packageName) {
    if (packageName.startsWith("run.ratchet.coordinator.jms")) {
      return "jms";
    }
    if (packageName.startsWith("run.ratchet.coordinator.postgresql")) {
      return "postgresql";
    }
    if (packageName.startsWith("run.ratchet.coordinator.infinispan")) {
      return "infinispan";
    }
    if (packageName.startsWith("run.ratchet.coordinator.hazelcast")) {
      return "hazelcast";
    }
    return null;
  }
}
