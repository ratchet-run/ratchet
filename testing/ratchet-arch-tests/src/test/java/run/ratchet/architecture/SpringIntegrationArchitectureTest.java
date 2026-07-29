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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.core.domain.Dependency;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaField;
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

/** Guards the only reference-implementation package exposed to Spring integrations. */
@AnalyzeClasses(packages = "run.ratchet", importOptions = ImportOption.DoNotIncludeTests.class)
public class SpringIntegrationArchitectureTest {

  private static final String RI_PACKAGE = "run.ratchet.ri";
  private static final String RUNTIME_PACKAGE = "run.ratchet.ri.runtime";
  private static final String SPRING_PACKAGE = "run.ratchet.spring";

  @Test
  void importerSeesRuntimeAndSpringClasses() {
    var imported =
        new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("run.ratchet");

    assertPackageNonEmpty(imported, RUNTIME_PACKAGE);
    assertPackageNonEmpty(imported, SPRING_PACKAGE);
  }

  @ArchTest
  static final ArchRule runtimePublicApiDoesNotExposeUnexportedRiTypes =
      classes()
          .that()
          .resideInAPackage(RUNTIME_PACKAGE + "..")
          .should(
              new ArchCondition<>("expose only exported RI types in public API signatures") {
                @Override
                public void check(JavaClass item, ConditionEvents events) {
                  for (JavaField field : item.getFields()) {
                    if (field.getModifiers().contains(JavaModifier.PUBLIC)) {
                      checkSignature(field, field.getAllInvolvedRawTypes(), events);
                    }
                  }
                  for (JavaCodeUnit codeUnit : item.getMethods()) {
                    if (codeUnit.getModifiers().contains(JavaModifier.PUBLIC)) {
                      checkSignature(codeUnit, codeUnit.getAllInvolvedRawTypes(), events);
                    }
                  }
                  for (JavaCodeUnit codeUnit : item.getConstructors()) {
                    if (codeUnit.getModifiers().contains(JavaModifier.PUBLIC)) {
                      checkSignature(codeUnit, codeUnit.getAllInvolvedRawTypes(), events);
                    }
                  }
                }
              })
          .because(
              "run.ratchet.ri.runtime is the RI's only exported package and its public API must "
                  + "not name opaque implementation types");

  @ArchTest
  static final ArchRule springCodeDependsOnlyOnTheExportedRuntimePackage =
      classes()
          .that()
          .resideInAPackage(SPRING_PACKAGE + "..")
          .should(
              new ArchCondition<>("depend only on the exported RI runtime package") {
                @Override
                public void check(JavaClass item, ConditionEvents events) {
                  for (Dependency dependency : item.getDirectDependenciesFromSelf()) {
                    if (isUnexportedRiType(dependency.getTargetClass())) {
                      events.add(
                          SimpleConditionEvent.violated(
                              dependency,
                              dependency.getDescription()
                                  + " reaches an unexported Ratchet RI package"));
                    }
                  }
                }
              })
          .because(
              "Spring integrations consume the portable runtime catalog instead of reaching into "
                  + "unexported RI implementation packages");

  private static void checkSignature(
      Object signature, Set<JavaClass> involvedTypes, ConditionEvents events) {
    for (JavaClass involvedType : involvedTypes) {
      if (isUnexportedRiType(involvedType)) {
        events.add(
            SimpleConditionEvent.violated(
                signature,
                signature
                    + " exposes unexported RI type "
                    + involvedType.getName()
                    + " in its public API"));
      }
    }
  }

  private static boolean isUnexportedRiType(JavaClass type) {
    String packageName = type.getPackageName();
    return (packageName.equals(RI_PACKAGE) || packageName.startsWith(RI_PACKAGE + "."))
        && !packageName.equals(RUNTIME_PACKAGE);
  }

  private static void assertPackageNonEmpty(Iterable<JavaClass> imported, String packageName) {
    long count = 0;
    for (JavaClass importedClass : imported) {
      if (importedClass.getPackageName().startsWith(packageName)) {
        count++;
      }
    }
    assertTrue(count > 0, "expected at least one class in " + packageName + ", found 0");
  }
}
