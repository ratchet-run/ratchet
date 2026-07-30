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

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import org.junit.jupiter.api.Test;

/**
 * Locks in transaction-boundary decisions Ratchet's portability depends on, so a future PR cannot
 * quietly undo them across the matrix of Jakarta EE servers.
 *
 * <ul>
 *   <li><b>No {@code @Transactional(SUPPORTS)} in the store layer.</b> A store method invoked
 *       outside a JTA transaction on EclipseLink-managed containers (Payara, GlassFish,
 *       OpenLiberty) leaks the borrowed pool connection with auto-commit disabled, leaving an open
 *       transaction that holds metadata locks on later writes. Store reads therefore stay on the
 *       class-level {@code REQUIRED} boundary instead of declaring {@code SUPPORTS}.
 *   <li><b>No {@code @Transactional} on the public API.</b> The {@code JobSchedulerService} surface
 *       is a pure contract: each method documents its transaction attribute in Javadoc, and the
 *       attribute is declared on the reference implementation, never on the exported interface.
 *   <li><b>Container transaction lookup stays behind the portable SPI.</b> RI services depend on
 *       {@code AfterCommitRegistrar}; only {@code JakartaAfterCommitRegistrar} may access the
 *       container's {@code TransactionSynchronizationRegistry}.
 * </ul>
 *
 * <p>The per-store {@code REQUIRES_NEW} lock-and-heartbeat rule lives in the store TCK's {@code
 * AbstractJobStoreTransactionBoundaryContract}, which the JPA stores opt into and which cleanly
 * exempts non-JTA stores; a package-scoped rule here could not make that distinction.
 */
@AnalyzeClasses(packages = "run.ratchet", importOptions = ImportOption.DoNotIncludeTests.class)
public class TransactionBoundaryArchitectureTest {

  private static final String API = "run.ratchet.api..";
  private static final String RI = "run.ratchet.ri..";
  private static final String STORE = "run.ratchet.store..";
  private static final String JAKARTA_AFTER_COMMIT_REGISTRAR =
      "run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar";
  private static final String DEFAULT_EXECUTOR_PROVIDER =
      "run.ratchet.ri.cdi.internal.DefaultExecutorProvider";

  /**
   * Defends against a vacuous pass: if the importer returns no store, API, or RI classes (a known
   * surefire/JaCoCo parallel-fork interaction), the rules below would pass with zero subjects.
   */
  @Test
  void archunitImporterSeesStoreApiAndRi() {
    JavaClasses imported =
        new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("run.ratchet");
    assertTrue(
        imported.size() > 50, "ArchUnit importer loaded only " + imported.size() + " classes");
    assertTrue(
        imported.stream().anyMatch(c -> c.getPackageName().startsWith("run.ratchet.store")),
        "no run.ratchet.store classes imported; the store rule would pass vacuously");
    assertTrue(
        imported.stream().anyMatch(c -> c.getPackageName().startsWith("run.ratchet.api")),
        "no run.ratchet.api classes imported; the API rule would pass vacuously");
    assertTrue(
        imported.stream().anyMatch(c -> c.getPackageName().startsWith("run.ratchet.ri")),
        "no run.ratchet.ri classes imported; the RI transaction rules would pass vacuously");
    assertTrue(
        imported.stream().anyMatch(c -> c.getName().equals(JAKARTA_AFTER_COMMIT_REGISTRAR)),
        JAKARTA_AFTER_COMMIT_REGISTRAR
            + " was not imported; its exclusive TSR access cannot be verified");
  }

  @ArchTest
  static final ArchRule storeMethodsDoNotDeclareSupportsTransaction =
      methods()
          .that()
          .areDeclaredInClassesThat()
          .resideInAPackage(STORE)
          .should(notUseSupportsTransaction())
          .because(
              "a @Transactional(SUPPORTS) store method invoked outside a JTA transaction on "
                  + "EclipseLink containers leaks the borrowed pool connection with auto-commit "
                  + "disabled; store reads stay on the class-level REQUIRED boundary")
          .allowEmptyShould(true);

  @ArchTest
  static final ArchRule publicApiMethodsDeclareNoTransactional =
      noMethods()
          .that()
          .areDeclaredInClassesThat()
          .resideInAPackage(API)
          .should()
          .beAnnotatedWith(Transactional.class)
          .because(
              "the JobSchedulerService API is a pure contract; a method's transaction attribute is "
                  + "documented in Javadoc and declared on the reference implementation, never on "
                  + "the exported interface");

  @ArchTest
  static final ArchRule publicApiTypesDeclareNoTransactional =
      noClasses()
          .that()
          .resideInAPackage(API)
          .should()
          .beAnnotatedWith(Transactional.class)
          .because(
              "transaction attributes belong on the reference implementation, not the exported API "
                  + "types");

  @ArchTest
  static final ArchRule transactionSynchronizationRegistryIsConfinedToJakartaRegistrar =
      noClasses()
          .that()
          .resideInAPackage(RI)
          .and(areNotJakartaAfterCommitRegistrar())
          .should()
          .dependOnClassesThat()
          .haveFullyQualifiedName(TransactionSynchronizationRegistry.class.getName())
          .because(
              "RI services use the portable AfterCommitRegistrar SPI; only "
                  + "JakartaAfterCommitRegistrar may access the container TSR");

  /**
   * ArchUnit cannot inspect a lookup call's string argument, so this rule confines JNDI lookup
   * calls to the transaction registrar and the existing managed-executor resolver. In particular,
   * an RI service cannot re-grow an inline {@code java:comp/TransactionSynchronizationRegistry}
   * lookup.
   */
  @ArchTest
  static final ArchRule jndiLookupsAreConfinedToRiResolvers =
      noClasses()
          .that()
          .resideInAPackage(RI)
          .and(areNotJndiLookupResolvers())
          .should()
          .callMethod(InitialContext.class, "doLookup", String.class)
          .orShould()
          .callMethod(InitialContext.class, "doLookup", Name.class)
          .orShould()
          .callMethod(InitialContext.class, "lookup", String.class)
          .orShould()
          .callMethod(InitialContext.class, "lookup", Name.class)
          .orShould()
          .callMethod(Context.class, "lookup", String.class)
          .orShould()
          .callMethod(Context.class, "lookup", Name.class)
          .because(
              "JNDI resource lookup is centralized in the Jakarta transaction registrar and the "
                  + "managed-executor resolver; RI services must not look up the TSR themselves");

  private static DescribedPredicate<JavaClass> areNotJakartaAfterCommitRegistrar() {
    return topLevelClassIsNot(
        JAKARTA_AFTER_COMMIT_REGISTRAR, "are not JakartaAfterCommitRegistrar");
  }

  private static DescribedPredicate<JavaClass> areNotJndiLookupResolvers() {
    return new DescribedPredicate<>("are not the documented RI JNDI resolvers") {
      @Override
      public boolean test(JavaClass clazz) {
        String topLevelClassName = topLevelClassName(clazz);
        return !topLevelClassName.equals(JAKARTA_AFTER_COMMIT_REGISTRAR)
            && !topLevelClassName.equals(DEFAULT_EXECUTOR_PROVIDER);
      }
    };
  }

  private static DescribedPredicate<JavaClass> topLevelClassIsNot(
      String excludedClassName, String description) {
    return new DescribedPredicate<>(description) {
      @Override
      public boolean test(JavaClass clazz) {
        return !topLevelClassName(clazz).equals(excludedClassName);
      }
    };
  }

  private static String topLevelClassName(JavaClass clazz) {
    return clazz.getName().split("\\$", 2)[0];
  }

  private static ArchCondition<JavaMethod> notUseSupportsTransaction() {
    return new ArchCondition<>("not be annotated @Transactional(SUPPORTS)") {
      @Override
      public void check(JavaMethod method, ConditionEvents events) {
        if (method.isAnnotatedWith(Transactional.class)
            && method.getAnnotationOfType(Transactional.class).value()
                == Transactional.TxType.SUPPORTS) {
          events.add(
              SimpleConditionEvent.violated(
                  method,
                  method.getFullName()
                      + " declares @Transactional(SUPPORTS); on EclipseLink containers this leaks"
                      + " the pooled connection when called outside a JTA transaction"));
        }
      }
    };
  }
}
