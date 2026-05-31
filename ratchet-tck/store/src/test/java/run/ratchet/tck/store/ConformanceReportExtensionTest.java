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
              .filter(name -> name.endsWith("Contract.class"))
              .map(name -> name.substring(0, name.length() - ".class".length()))
              .filter(name -> ConformanceLevel.forContract(name) == null)
              .sorted()
              .toList();
    }

    assertEquals(List.of(), unregisteredContracts, "all Abstract*Contract classes are registered");
  }

  @Test
  void conformanceLevel_unknownContract_returnsNull() {
    assertNull(ConformanceLevel.forContract("SomeRandomClass"));
  }
}
