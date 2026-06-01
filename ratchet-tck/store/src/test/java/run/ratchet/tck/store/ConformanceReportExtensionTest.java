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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConformanceReportExtensionTest {

  // Minimal stand-ins to exercise the superclass-chain walk without spinning up real containers
  static class FakeCrudTest extends AbstractJobCrudStoreContract {

    @Override
    public run.ratchet.store.spi.JobStore store() {
      return null;
    }

    @Override
    public run.ratchet.store.entity.JobEntity newPendingJob() {
      return null;
    }

    @Override
    public run.ratchet.store.entity.JobEntity newBatchParentJob() {
      return null;
    }

    @Override
    public void cleanupStore() {}
  }

  static class FakeLockTest extends AbstractLockStoreContract {

    @Override
    public run.ratchet.store.spi.JobStore store() {
      return null;
    }

    @Override
    public run.ratchet.store.entity.JobEntity newPendingJob() {
      return null;
    }

    @Override
    public run.ratchet.store.entity.JobEntity newBatchParentJob() {
      return null;
    }

    @Override
    public void cleanupStore() {}
  }

  static class UnrelatedTest {}

  @Test
  void findContractSimpleName_directSubclass_returnsContractName() {
    String result = ConformanceReportExtension.findContractSimpleName(FakeCrudTest.class.getName());
    assertEquals("AbstractJobCrudStoreContract", result);
  }

  @Test
  void findContractSimpleName_lockContract_returnsCorrectName() {
    String result = ConformanceReportExtension.findContractSimpleName(FakeLockTest.class.getName());
    assertEquals("AbstractLockStoreContract", result);
  }

  @Test
  void findContractSimpleName_unrelatedClass_returnsNull() {
    String result =
        ConformanceReportExtension.findContractSimpleName(UnrelatedTest.class.getName());
    assertNull(result);
  }

  @Test
  void findContractSimpleName_unknownClassName_returnsNull() {
    String result = ConformanceReportExtension.findContractSimpleName("com.example.DoesNotExist");
    assertNull(result);
  }

  @Test
  void conformanceLevel_coreContractsAreRecognized() {
    assertEquals(
        ConformanceLevel.CORE, ConformanceLevel.forContract("AbstractJobCrudStoreContract"));
    assertEquals(ConformanceLevel.CORE, ConformanceLevel.forContract("AbstractLockStoreContract"));
  }

  @Test
  void conformanceLevel_allAbstractContractsAreRegistered() throws Exception {
    // Anchor the scan on a class that lives beside the contracts in main output. Resolving the
    // package via the context classloader returned the test-classes copy of the directory, which
    // holds no Abstract*Contract.class, so the scan saw nothing and passed vacuously.
    var resource = ConformanceLevel.class.getResource("ConformanceLevel.class");
    var packageDir = Paths.get(resource.toURI()).getParent();

    List<String> unregisteredContracts;
    try (var classes = Files.list(packageDir)) {
      unregisteredContracts =
          classes
              .map(path -> path.getFileName().toString())
              .filter(name -> name.startsWith("Abstract"))
              .filter(name -> name.endsWith(".class"))
              .filter(name -> !name.contains("$"))
              .map(name -> name.substring(0, name.length() - ".class".length()))
              .filter(ConformanceReportExtensionTest::declaresTestMethods)
              .filter(name -> ConformanceLevel.forContract(name) == null)
              .sorted()
              .toList();
    }

    assertEquals(
        List.of(),
        unregisteredContracts,
        "every abstract store contract (any Abstract* class declaring @Test methods) must be"
            + " registered in ConformanceLevel; keying on @Test rather than the *Contract suffix"
            + " stops a contract from hiding under a different name");
  }

  /**
   * Treats an {@code Abstract*} class as a contract when it declares at least one {@code @Test}
   * method. This is what makes MISSING-detection robust against class-name drift: the original
   * {@code AbstractSignalContractTest} was a full store contract but escaped the report because the
   * scan matched only the {@code *Contract} suffix.
   */
  private static boolean declaresTestMethods(String simpleName) {
    Class<?> type;
    try {
      type =
          Class.forName(
              "run.ratchet.tck.store." + simpleName,
              false,
              ConformanceReportExtensionTest.class.getClassLoader());
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

  @Test
  void conformanceLevel_unknownContract_returnsNull() {
    assertNull(ConformanceLevel.forContract("SomeRandomClass"));
  }
}
