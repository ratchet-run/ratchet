package run.ratchet.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

/**
 * Locks in the two transaction-boundary decisions Ratchet's portability depends on, so a future PR
 * cannot quietly undo them across the matrix of Jakarta EE servers.
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
 * </ul>
 *
 * <p>The per-store {@code REQUIRES_NEW} lock-and-heartbeat rule lives in the store TCK's {@code
 * AbstractJobStoreTransactionBoundaryContract}, which the JPA stores opt into and which cleanly
 * exempts non-JTA stores; a package-scoped rule here could not make that distinction.
 */
@AnalyzeClasses(packages = "run.ratchet", importOptions = ImportOption.DoNotIncludeTests.class)
public class TransactionBoundaryArchitectureTest {

  private static final String API = "run.ratchet.api..";
  private static final String STORE = "run.ratchet.store..";

  /**
   * Defends against a vacuous pass: if the importer returns no store or API classes (a known
   * surefire/JaCoCo parallel-fork interaction), the rules below would pass with zero subjects.
   */
  @Test
  void archunitImporterSeesStoreAndApi() {
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
