package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
  void conformanceLevel_unknownContract_returnsNull() {
    assertNull(ConformanceLevel.forContract("SomeRandomClass"));
  }
}
