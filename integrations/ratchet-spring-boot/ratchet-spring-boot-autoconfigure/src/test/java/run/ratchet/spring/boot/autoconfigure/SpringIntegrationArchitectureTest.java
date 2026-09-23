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
package run.ratchet.spring.boot.autoconfigure;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(
    packages = "run.ratchet.spring.boot.autoconfigure",
    importOptions = ImportOption.DoNotIncludeTests.class)
class SpringIntegrationArchitectureTest {
  @ArchTest
  static void importsProductionConfiguration(JavaClasses classes) {
    for (Class<?> type :
        new Class<?>[] {
          RatchetAutoConfiguration.class,
          RatchetEngineAutoConfiguration.class,
          SpringAfterCommitRegistrar.class,
          RatchetJpaIsolationAutoConfiguration.class,
          RatchetJpaIsolationAotProcessor.class
        }) {
      assertTrue(
          classes.stream().anyMatch(imported -> imported.getName().equals(type.getName())),
          () -> "Architecture rules must inspect " + type.getName());
    }
  }

  @ArchTest
  static final ArchRule springWiringDoesNotUseCdi =
      noClasses()
          .that()
          .resideInAPackage("run.ratchet.spring.boot.autoconfigure..")
          .should()
          .dependOnClassesThat()
          .resideInAPackage("jakarta.enterprise..")
          .because("Spring wiring must work without a CDI container");

  @ArchTest
  static final ArchRule engineConfigurationDoesNotChoosePersistence =
      noClasses()
          .that()
          .resideInAPackage("run.ratchet.spring.boot.autoconfigure..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "run.ratchet.spring.boot.autoconfigure.jpa..",
              "run.ratchet.spring.boot.autoconfigure.mongodb..",
              "run.ratchet.store.mysql..",
              "run.ratchet.store.postgresql..",
              "run.ratchet.store.oracle..",
              "run.ratchet.store.sqlserver..",
              "run.ratchet.store.mongodb..",
              "org.springframework.data..",
              "org.hibernate..",
              "org.eclipse.persistence..",
              "com.mongodb..")
          .because("store and provider selection belongs in the JPA or MongoDB integration module");

  @ArchTest
  static final ArchRule onlyApplicationMappingIsolationUsesJpaInfrastructure =
      noClasses()
          .that()
          .resideInAPackage("run.ratchet.spring.boot.autoconfigure..")
          .and()
          .resideOutsideOfPackage("run.ratchet.spring.boot.autoconfigure.internal.jpa..")
          .and()
          .haveNameNotMatching(
              "run\\.ratchet\\.spring\\.boot\\.autoconfigure\\.RatchetJpaIsolation"
                  + "(?:AutoConfiguration(?:\\$.*)?|AotProcessor)")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "jakarta.persistence..",
              "org.springframework.orm..",
              "run.ratchet.spring.boot.autoconfigure.internal.jpa..")
          .because(
              "only the optional application mapping isolation bridge may use JPA infrastructure; "
                  + "the engine must remain persistence independent");
}
