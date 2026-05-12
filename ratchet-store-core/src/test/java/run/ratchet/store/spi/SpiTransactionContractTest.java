package run.ratchet.store.spi;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class SpiTransactionContractTest {

  private static final Path SPI_ROOT =
      Path.of("src/main/java/run/ratchet/store/spi").toAbsolutePath();

  @Test
  void ownedStoreSpiFilesDocumentTransactionAttributes() throws Exception {
    assertDocumentsTransactionAttributes("ArchiveStore.java", 6);
    assertDocumentsTransactionAttribute("BatchMetricsStore.java");
    assertDocumentsTransactionAttribute("BatchStore.java");
    assertDocumentsTransactionAttribute("DlqAlertStore.java");
    assertDocumentsTransactionAttributes("ExecutionStore.java", 5);
    assertDocumentsTransactionAttribute("JobBatchStatusStore.java");
    assertDocumentsTransactionAttribute("JobBulkStore.java");
    assertDocumentsTransactionAttribute("JobClaimStore.java");
    assertDocumentsTransactionAttribute("JobCrudStore.java");
    assertDocumentsTransactionAttribute("JobLogStore.java");
    assertDocumentsTransactionAttribute("JobPauseStore.java");
    assertDocumentsTransactionAttributes("JobQueryStore.java", 2);
    assertDocumentsTransactionAttribute("JobRetryStore.java");
    assertDocumentsTransactionAttribute("JobTerminalStore.java");
    assertDocumentsTransactionAttributes("LockStore.java", 3);
    assertDocumentsTransactionAttribute("NodeStore.java");
    assertDocumentsTransactionAttribute("ResourcePermitStore.java");
    assertDocumentsTransactionAttribute("SignalStore.java");
    assertDocumentsTransactionAttribute("TagStore.java");
    assertDocumentsTransactionAttributes("WorkflowConditionStore.java", 9);
  }

  private static void assertDocumentsTransactionAttribute(String fileName) throws Exception {
    String source = Files.readString(SPI_ROOT.resolve(fileName));

    assertTrue(
        source.contains("Transaction attribute:"),
        () -> fileName + " should document store SPI transaction attributes");
  }

  private static void assertDocumentsTransactionAttributes(String fileName, int expectedCount)
      throws Exception {
    String source = Files.readString(SPI_ROOT.resolve(fileName));
    int count = source.split("Transaction attribute:", -1).length - 1;

    assertTrue(
        count >= expectedCount,
        () ->
            fileName
                + " should document at least "
                + expectedCount
                + " transaction attributes but had "
                + count);
  }
}
