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
package run.ratchet.spring.boot.autoconfigure.jpa;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(
    packages = "run.ratchet.spring.boot",
    importOptions = ImportOption.DoNotIncludeTests.class)
class JpaProviderArchitectureTest {
  @ArchTest
  static final ArchRule springProviderApisStayInMappingAdapter =
      noClasses()
          .that()
          .resideInAPackage("run.ratchet.spring.boot..")
          .and()
          .doNotHaveFullyQualifiedName(
              "run.ratchet.spring.boot.autoconfigure.jpa.HibernateJpaMappings")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("org.hibernate..", "org.eclipse.persistence..")
          .because(
              "provider-specific APIs belong in the optional mapping adapter, not general Spring wiring");
}
