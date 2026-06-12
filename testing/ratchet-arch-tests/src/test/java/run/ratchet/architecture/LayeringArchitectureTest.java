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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

/**
 * Locks in the module boundaries Ratchet's Jakarta EE spec-candidacy posture depends on. Every rule
 * guards an invariant that holds today; the value is in catching the first PR that would silently
 * break it.
 *
 * <p>The smoke-guards below defend against two known vacuous-pass modes:
 *
 * <ul>
 *   <li>Total importer shrinkage (surefire parallel + JaCoCo agent interaction): the {@link
 *       #archunitImporterSeesAllModules()} test asserts the importer loaded classes from every
 *       module under analysis. If a coordinator or store-impl JAR drops off the test classpath,
 *       rules whose subject is that package would otherwise pass with zero matches.
 *   <li>Per-package shrinkage (refactor empties an {@code .internal} package): each {@link
 *       #internalPackageSubjectCountsArePositive()} assertion proves the relevant {@code .internal}
 *       package is non-empty before the {@code @ArchTest} rules below run.
 * </ul>
 */
@AnalyzeClasses(packages = "run.ratchet", importOptions = ImportOption.DoNotIncludeTests.class)
public class LayeringArchitectureTest {

  private static final String API = "run.ratchet.api..";
  private static final String SPI = "run.ratchet.spi..";
  private static final String STORE_CORE = "run.ratchet.store..";
  private static final String STORE_MYSQL = "run.ratchet.store.mysql..";
  private static final String STORE_POSTGRESQL = "run.ratchet.store.postgresql..";
  private static final String STORE_MONGODB = "run.ratchet.store.mongodb..";
  private static final String RI = "run.ratchet.ri..";
  private static final String COORDINATOR = "run.ratchet.coordinator..";
  private static final String BLOCKS = "run.ratchet.blocks..";

  /**
   * Defends against total importer shrinkage. Asserts the importer found classes from every module
   * the rules below reason about, so a missing dependency in {@code ratchet-arch-tests/pom.xml}
   * surfaces here instead of silently disarming a rule.
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
  }

  /**
   * Defends against per-package shrinkage. Each {@code .internal} package the rules below fence
   * must be non-empty, otherwise the rule passes vacuously for that package even when the importer
   * sees plenty of other classes.
   */
  @Test
  void internalPackageSubjectCountsArePositive() {
    JavaClasses imported =
        new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("run.ratchet");
    assertPackageNonEmpty(imported, "run.ratchet.api.internal");
    assertPackageNonEmpty(imported, "run.ratchet.ri.core.internal");
    assertPackageNonEmpty(imported, "run.ratchet.ri.cdi.internal");
    assertPackageNonEmpty(imported, "run.ratchet.coordinator.common.internal");
  }

  private static void assertPackageNonEmpty(JavaClasses imported, String packageName) {
    long count = imported.stream().filter(c -> c.getPackageName().startsWith(packageName)).count();
    assertTrue(count > 0, "expected at least one class in " + packageName + ", found 0");
  }

  // --- Core must never depend on the optional blocks extension ---

  /**
   * {@code run.ratchet.blocks..} is the optional low-code extension layered ON TOP of the core:
   * blocks depends on the API and SPI seams ({@code InvocationSubmissionService}, {@code
   * JobExtensionStore}, {@code PreExecutionArgResolver}), never the reverse. A single core-side
   * reference into the blocks package would make the optional module mandatory. The rule's subject
   * packages are non-empty today, so it cannot pass vacuously even while the blocks module itself
   * does not exist yet.
   */
  @ArchTest
  static final ArchRule coreDoesNotDependOnBlocksExtension =
      noClasses()
          .that()
          .resideInAnyPackage(API, SPI, STORE_CORE, RI, COORDINATOR)
          .should()
          .dependOnClassesThat()
          .resideInAPackage(BLOCKS)
          .because(
              "ratchet-blocks is an optional extension; the core must never depend on it"
                  + " (dependency direction: blocks -> api, never the reverse)");

  // --- API + store-core must not depend on the reference implementation ---

  /**
   * {@code ratchet-api} and {@code ratchet-store-core} must not depend on {@code run.ratchet.ri..}.
   * The API is the artifact other vendors implement; a single back-reference into the RI would
   * silently couple the spec to one implementation. JPMS exports alone don't catch this because
   * every Jakarta EE deployment in the test matrix runs on the classpath, not the module path.
   */
  @ArchTest
  static final ArchRule apiAndStoreCoreDoNotDependOnReferenceImplementation =
      noClasses()
          .that()
          .resideInAnyPackage(API, SPI, STORE_CORE)
          .and()
          .resideOutsideOfPackage(RI)
          .should()
          .dependOnClassesThat()
          .resideInAPackage(RI)
          .because(
              "the API is the spec-candidacy artifact other vendors implement; back-references "
                  + "into the reference implementation couple the spec to one impl");

  /**
   * {@code ratchet-api} (api + spi) must not depend on {@code run.ratchet.store..} or {@code
   * run.ratchet.coordinator..}. The API stays the apex of the dependency graph in both directions.
   * Store impls and coordinators depend on the API, never the other way around.
   */
  @ArchTest
  static final ArchRule apiDoesNotDependOnStoreOrCoordinator =
      noClasses()
          .that()
          .resideInAnyPackage(API, SPI)
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(STORE_CORE, COORDINATOR)
          .because(
              "ratchet-api stays the apex of the dependency graph; store and coordinator "
                  + "modules depend on it, never the reverse");

  // --- Coordinator and store-impl modules must not depend on the RI engine ---

  /**
   * Coordinator modules and SQL/document store impls must not depend on {@code run.ratchet.ri..}.
   * Pluggability story for spec candidacy: third-party coordinators and stores compile against
   * {@code ratchet-api} and {@code ratchet-store-core} only.
   */
  @ArchTest
  static final ArchRule coordinatorAndStoreImplsDoNotDependOnReferenceImplementation =
      noClasses()
          .that()
          .resideInAnyPackage(COORDINATOR, STORE_MYSQL, STORE_POSTGRESQL, STORE_MONGODB)
          .should()
          .dependOnClassesThat()
          .resideInAPackage(RI)
          .because(
              "third-party coordinators and stores must compile against ratchet-api and "
                  + "ratchet-store-core only; reach-in to the reference implementation breaks "
                  + "the pluggability contract");

  // --- Cross-module .internal reach-in ---

  /**
   * {@code run.ratchet.api.internal..} is qualified-exported to {@code run.ratchet.ri} at the JPMS
   * level. The same restriction applies on the classpath, where module-info exports are not
   * enforced.
   */
  @ArchTest
  static final ArchRule apiInternalIsConsumedOnlyByReferenceImplementation =
      classes()
          .that()
          .resideInAPackage("run.ratchet.api.internal..")
          .should()
          .onlyHaveDependentClassesThat()
          .resideInAnyPackage("run.ratchet.api..", RI)
          .because(
              "run.ratchet.api.internal is exported to run.ratchet.ri only in module-info; "
                  + "classpath consumers must respect the same boundary");

  /**
   * RI-internal sub-packages stay inside the RI module. Coordinators, store impls, and tests should
   * not reach into {@code run.ratchet.ri.core.internal} or {@code run.ratchet.ri.cdi.internal}.
   */
  @ArchTest
  static final ArchRule riInternalsAreConsumedOnlyWithinTheReferenceImplementation =
      classes()
          .that()
          .resideInAnyPackage("run.ratchet.ri.core.internal..", "run.ratchet.ri.cdi.internal..")
          .should()
          .onlyHaveDependentClassesThat()
          .resideInAPackage(RI)
          .because(
              "RI-internal sub-packages are implementation detail; reach-in from coordinators "
                  + "or store impls breaks the pluggability boundary");

  /**
   * {@code run.ratchet.coordinator.common.internal..} is shared between sibling coordinator
   * implementations (e.g. {@code coordinator-jms} depends on {@code
   * coordinator-common.internal.NotifyPayloadCodec}). The package stays inside the coordinator
   * family — RI and store-impl modules should not reach into it.
   */
  @ArchTest
  static final ArchRule coordinatorCommonInternalsStayInCoordinatorFamily =
      classes()
          .that()
          .resideInAPackage("run.ratchet.coordinator.common.internal..")
          .should()
          .onlyHaveDependentClassesThat()
          .resideInAPackage(COORDINATOR)
          .because(
              "coordinator.common.internal is intra-family glue between sibling coordinator "
                  + "modules; reach-in from outside the coordinator family is a layering bug");

  // --- JPA purity for SQL stores ---

  /**
   * Classes in the SQL store modules and shared store-core main sources must not import
   * provider-specific JPA APIs. This is a hard project constraint — a Hibernate-specific annotation
   * in store-core would silently break Payara/EclipseLink in production. The TCK tests use
   * Hibernate as the JPA provider, which is fine because they live in {@code src/test/} and the
   * importer is configured with {@code DoNotIncludeTests}.
   */
  @ArchTest
  static final ArchRule sqlStoresUseStandardJpaOnly =
      noClasses()
          .that()
          .resideInAnyPackage(STORE_CORE, STORE_MYSQL, STORE_POSTGRESQL)
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("org.hibernate..", "org.eclipse.persistence..")
          .because(
              "JPA-only is a hard constraint; provider-specific annotations or APIs silently "
                  + "couple the store to one provider and break the others in production");

  /**
   * Classes annotated {@code @jakarta.persistence.Entity} may depend only on the standard JPA API,
   * the JDK, and other Ratchet types. Catches a Hibernate-specific annotation on an entity field
   * even if its import is on-demand or wildcard.
   *
   * <p>Scoped to {@code @Entity} (not the {@code .entity} package) because the package also holds
   * JPA value objects persisted through {@link jakarta.persistence.AttributeConverter}s (e.g.
   * {@code JobPayload}), which legitimately depend on ASM for descriptor parsing — they are not
   * part of the entity-mapping surface.
   */
  @ArchTest
  static final ArchRule entitiesDependOnlyOnStandardJpa =
      classes()
          .that()
          .areAnnotatedWith(jakarta.persistence.Entity.class)
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(
              "jakarta.persistence..", "jakarta.annotation..", "java..", "run.ratchet..")
          .because(
              "entity classes are the most fragile JPA surface; any non-standard dependency "
                  + "here couples the persistence model to one provider");

  // --- Logging consistency ---

  /**
   * The RI, store, and coordinator modules use JBoss Logging exclusively. Other logging frameworks
   * (JUL, SLF4J, Log4j, JCL) bypass the project's chosen logger and skew log routing in EE
   * deployments.
   *
   * <p>Carve-outs by package, not by file path:
   *
   * <ul>
   *   <li>{@code run.ratchet.api..} and {@code run.ratchet.spi..} intentionally use JUL to keep the
   *       public API dependency-free; this is documented in {@code module-info.java}.
   *   <li>{@code run.ratchet.tck..} is allowed to use JUL because the JDBC {@code DataSource}
   *       contract returns {@code java.util.logging.Logger}.
   * </ul>
   */
  @ArchTest
  static final ArchRule loggingFrameworkIsJbossLoggingInImplementationModules =
      noClasses()
          .that()
          .resideInAnyPackage(RI, STORE_CORE, COORDINATOR)
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "java.util.logging..",
              "org.slf4j..",
              "org.apache.logging.log4j..",
              "org.apache.commons.logging..")
          .because(
              "the project standardizes on JBoss Logging in implementation modules; drift skews "
                  + "log routing in EE deployments and bypasses the chosen logger");

  // --- @Incubating fencing ---

  /**
   * The {@code @Incubating} annotation marks unstable API/SPI surface area. It lives in {@code
   * ratchet-api} and is currently applied in {@code ratchet-api} and {@code ratchet-store-core}
   * public types. It should never leak onto an implementation class — implementation classes are
   * not part of the spec contract and have no need for a stability marker.
   */
  @ArchTest
  static final ArchRule incubatingMarkerStaysOnApiAndStoreCoreTypes =
      classes()
          .that()
          .areAnnotatedWith(run.ratchet.api.Incubating.class)
          .should()
          .resideInAnyPackage(API, SPI, STORE_CORE)
          .because(
              "@Incubating is a stability contract on the API/SPI surface; applying it to "
                  + "implementation classes confuses what is and is not part of the spec");
}
