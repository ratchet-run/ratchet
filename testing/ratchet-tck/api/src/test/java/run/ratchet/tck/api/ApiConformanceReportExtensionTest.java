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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.util.AbstractConformanceReportExtension.ContractGroup;

class ApiConformanceReportExtensionTest {

  /**
   * Every shipped {@code Abstract*Contract} in the API tier must appear in the report catalog.
   * Missing-contract detection and the final "Ratchet API Compatible" verdict are driven entirely
   * by the catalog, so a contract left out runs but never counts toward PASS/FAIL accounting. The
   * scan is anchored on a contract class's own resource so it reads the main output directory where
   * the contracts live, not the test-classes copy of the package.
   */
  @Test
  void allApiContractsAreCataloged() throws Exception {
    var resource =
        AbstractJobLifecycleContract.class.getResource("AbstractJobLifecycleContract.class");
    var packageDir = Paths.get(resource.toURI()).getParent();

    Set<String> cataloged =
        new ApiConformanceReportExtension()
            .contractGroups().stream()
                .flatMap(
                    (ContractGroup g) ->
                        Stream.concat(g.contracts().stream(), g.optionalContracts().stream()))
                .collect(Collectors.toSet());

    List<String> uncataloged;
    try (var classes = Files.list(packageDir)) {
      uncataloged =
          classes
              .map(path -> path.getFileName().toString())
              .filter(name -> name.startsWith("Abstract"))
              .filter(name -> name.endsWith(".class"))
              .filter(name -> !name.contains("$"))
              .map(name -> name.substring(0, name.length() - ".class".length()))
              .filter(ApiConformanceReportExtensionTest::declaresTestMethods)
              .filter(name -> !cataloged.contains(name))
              .sorted()
              .toList();
    }

    assertEquals(
        List.of(),
        uncataloged,
        "every API contract (any Abstract* class declaring @Test methods) must be cataloged;"
            + " keying on @Test rather than the *Contract suffix stops a contract from hiding under"
            + " a different name");
  }

  /**
   * Treats an {@code Abstract*} class as a contract when it declares at least one {@code @Test}
   * method. Keying on {@code @Test} rather than the {@code *Contract} suffix keeps a renamed
   * contract (e.g. an {@code Abstract*Test}) from silently escaping the catalog, the same way the
   * store tier guards its own scan.
   */
  private static boolean declaresTestMethods(String simpleName) {
    Class<?> type;
    try {
      type =
          Class.forName(
              "run.ratchet.tck.api." + simpleName,
              false,
              ApiConformanceReportExtensionTest.class.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
    for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
      for (var method : c.getDeclaredMethods()) {
        if (method.isAnnotationPresent(Test.class)) {
          return true;
        }
      }
    }
    return false;
  }
}
