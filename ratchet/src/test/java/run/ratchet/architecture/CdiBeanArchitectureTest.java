package run.ratchet.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ConversationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.context.SessionScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionScoped;
import java.lang.annotation.Annotation;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Catches Weld bootstrap failures (e.g., WELD-001435 "not proxyable") at unit-test time so the
 * matrix of integration tests across Jakarta EE servers doesn't have to.
 */
@AnalyzeClasses(packages = "run.ratchet", importOptions = ImportOption.DoNotIncludeTests.class)
public class CdiBeanArchitectureTest {

  private static final Set<Class<? extends Annotation>> NORMAL_SCOPES =
      Set.of(
          ApplicationScoped.class,
          RequestScoped.class,
          SessionScoped.class,
          ConversationScoped.class,
          TransactionScoped.class);

  /**
   * Defensive smoke-guard: vacuous rule passes happen when the ArchUnit importer returns zero
   * classes (a known surefire/JaCoCo/parallel interaction). If this assertion fires, every
   * {@code @ArchTest} rule below is meaningless.
   */
  @Test
  void archunit_importer_loads_production_classes() {
    JavaClasses imported =
        new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("run.ratchet");
    assertTrue(
        imported.size() > 50,
        "ArchUnit importer loaded "
            + imported.size()
            + " classes; rules below would pass vacuously. "
            + "If surefire moved to parallel forks with JaCoCo attached, see "
            + "~/kb/wiki/decisions/2026-05-29-nets-archunit-classpath-import-empty.md");
  }

  /**
   * Rule 1: every normal-scoped concrete bean class must declare a no-arg constructor with
   * visibility &ge; package-private. Weld generates a client-proxy subclass and needs the no-arg
   * constructor to invoke {@code super()}. Private no-arg is unproxyable; non-private is fine
   * regardless of how the canonical {@code @Inject} constructor is declared.
   */
  @ArchTest
  static final ArchRule normalScopedBeansDeclareNoArgConstructor =
      classes()
          .that(areConcrete())
          .and(isNotNonStaticInner())
          .and(areAnnotatedWithNormalScope())
          .should(haveNonPrivateNoArgConstructor())
          .because(
              "Weld must instantiate a client-proxy subclass via a no-arg constructor (CDI 4.0 "
                  + "§3.15); without one, deployment fails with WELD-001435");

  /**
   * Rule 2: every {@code @Produces} method whose declared scope is a normal scope must return a
   * type that itself declares a no-arg constructor. The bug class is methods like
   * {@code @Produces @ApplicationScoped PoolRegistry poolRegistry()} where the scope is on the
   * producer but the returned class has no no-arg constructor. Skips interface and abstract return
   * types because Weld can proxy those without a no-arg constructor on the declared type.
   */
  @ArchTest
  static final ArchRule producerMethodReturnTypesAreProxyable =
      methods()
          .that()
          .areAnnotatedWith(Produces.class)
          .and(areAnnotatedWithNormalScope_method())
          .should(returnConcreteTypeWithNonPrivateNoArgConstructor())
          .because(
              "the producer's return type is the bean class for proxy generation; if it is "
                  + "concrete without a no-arg constructor, Weld can't subclass it (WELD-001435)");

  /**
   * Rule 3: normal-scoped bean classes must not be {@code final} (and therefore must not be {@code
   * record}s, which are implicitly final). Weld generates a subclass for the client proxy and
   * cannot extend a final class.
   */
  @ArchTest
  static final ArchRule normalScopedBeansAreNotFinal =
      classes()
          .that(areConcrete())
          .and(areAnnotatedWithNormalScope())
          .should()
          .notHaveModifier(JavaModifier.FINAL)
          .because("Weld cannot extend a final class to create the client-proxy subclass");

  /**
   * Rule 4: a {@code @Dependent} class with multiple constructors must have either exactly one
   * {@code @Inject}-annotated constructor or a no-arg constructor. CDI 4.0 §3.3 makes this a
   * deployment problem.
   */
  @ArchTest
  static final ArchRule dependentBeansResolveAmbiguousConstructors =
      classes()
          .that(areConcrete())
          .and()
          .areAnnotatedWith(Dependent.class)
          .should(haveResolvableConstructor())
          .because(
              "CDI 4.0 §3.3 requires exactly one @Inject constructor or a no-arg constructor "
                  + "when a managed bean class declares more than one constructor")
          .allowEmptyShould(true);

  /**
   * Rule 5: a non-static member class cannot be a normal-scoped CDI bean. Its synthesized no-arg
   * constructor implicitly requires an outer-class instance, so Weld cannot proxy it.
   */
  @ArchTest
  static final ArchRule nonStaticInnerClassesAreNotNormalScopedBeans =
      classes()
          .that(isNonStaticInner())
          .should(notBeAnnotatedWithNormalScope())
          .because(
              "non-static member classes carry an implicit outer reference; Weld cannot "
                  + "instantiate a proxy subclass via a synthetic enclosing-instance constructor");

  // --- predicates ---

  private static DescribedPredicate<JavaClass> areConcrete() {
    return new DescribedPredicate<>("are concrete") {
      @Override
      public boolean test(JavaClass clazz) {
        return !clazz.getModifiers().contains(JavaModifier.ABSTRACT) && !clazz.isInterface();
      }
    };
  }

  private static DescribedPredicate<JavaClass> areAnnotatedWithNormalScope() {
    return new DescribedPredicate<>("are annotated with a normal scope") {
      @Override
      public boolean test(JavaClass clazz) {
        return NORMAL_SCOPES.stream().anyMatch(clazz::isAnnotatedWith);
      }
    };
  }

  private static DescribedPredicate<JavaMethod> areAnnotatedWithNormalScope_method() {
    return new DescribedPredicate<>("are annotated with a normal scope") {
      @Override
      public boolean test(JavaMethod method) {
        return NORMAL_SCOPES.stream().anyMatch(method::isAnnotatedWith);
      }
    };
  }

  private static DescribedPredicate<JavaClass> isNonStaticInner() {
    return new DescribedPredicate<>("are non-static member classes") {
      @Override
      public boolean test(JavaClass clazz) {
        return clazz.isInnerClass() && !clazz.getModifiers().contains(JavaModifier.STATIC);
      }
    };
  }

  private static DescribedPredicate<JavaClass> isNotNonStaticInner() {
    return new DescribedPredicate<>("are not non-static member classes") {
      @Override
      public boolean test(JavaClass clazz) {
        return !(clazz.isInnerClass() && !clazz.getModifiers().contains(JavaModifier.STATIC));
      }
    };
  }

  // --- conditions ---

  private static ArchCondition<JavaClass> haveNonPrivateNoArgConstructor() {
    return new ArchCondition<>("have a non-private no-arg constructor") {
      @Override
      public void check(JavaClass clazz, ConditionEvents events) {
        boolean ok =
            clazz.getConstructors().stream()
                .anyMatch(c -> c.getRawParameterTypes().isEmpty() && !isPrivate(c));
        if (!ok) {
          events.add(
              SimpleConditionEvent.violated(
                  clazz,
                  clazz.getFullName()
                      + " is normal-scoped but has no non-private no-arg constructor; "
                      + "Weld cannot subclass it for the client proxy"));
        }
      }
    };
  }

  private static ArchCondition<JavaMethod> returnConcreteTypeWithNonPrivateNoArgConstructor() {
    return new ArchCondition<>("return a concrete type that has a non-private no-arg constructor") {
      @Override
      public void check(JavaMethod method, ConditionEvents events) {
        JavaClass returnType = method.getRawReturnType();
        if (returnType.isInterface() || returnType.getModifiers().contains(JavaModifier.ABSTRACT)) {
          return;
        }
        if (!returnType.getPackageName().startsWith("run.ratchet")) {
          return;
        }
        boolean ok =
            returnType.getConstructors().stream()
                .anyMatch(c -> c.getRawParameterTypes().isEmpty() && !isPrivate(c));
        if (!ok) {
          events.add(
              SimpleConditionEvent.violated(
                  method,
                  method.getFullName()
                      + " produces normal-scoped "
                      + returnType.getFullName()
                      + " but that class has no non-private no-arg constructor; "
                      + "Weld cannot subclass it for the client proxy"));
        }
      }
    };
  }

  private static ArchCondition<JavaClass> haveResolvableConstructor() {
    return new ArchCondition<>(
        "have exactly one @Inject constructor or a no-arg constructor when more than one"
            + " constructor is declared") {
      @Override
      public void check(JavaClass clazz, ConditionEvents events) {
        Set<JavaConstructor> ctors = clazz.getConstructors();
        if (ctors.size() <= 1) {
          return;
        }
        long injectCount = ctors.stream().filter(c -> c.isAnnotatedWith(Inject.class)).count();
        boolean hasNoArg = ctors.stream().anyMatch(c -> c.getRawParameterTypes().isEmpty());
        if (injectCount != 1 && !hasNoArg) {
          events.add(
              SimpleConditionEvent.violated(
                  clazz,
                  clazz.getFullName()
                      + " is @Dependent with "
                      + ctors.size()
                      + " constructors but none is @Inject and none is no-arg; "
                      + "CDI cannot pick a constructor"));
        }
      }
    };
  }

  private static ArchCondition<JavaClass> notBeAnnotatedWithNormalScope() {
    return new ArchCondition<>("not be annotated with a normal scope") {
      @Override
      public void check(JavaClass clazz, ConditionEvents events) {
        NORMAL_SCOPES.stream()
            .filter(clazz::isAnnotatedWith)
            .findFirst()
            .ifPresent(
                scope ->
                    events.add(
                        SimpleConditionEvent.violated(
                            clazz,
                            clazz.getFullName()
                                + " is a non-static member class annotated @"
                                + scope.getSimpleName()
                                + "; Weld cannot proxy a class that requires an enclosing"
                                + " instance")));
      }
    };
  }

  private static boolean isPrivate(JavaConstructor constructor) {
    return constructor.getModifiers().contains(JavaModifier.PRIVATE);
  }
}
