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
    assertDocumentsTransactionAttribute("BatchMetricsStore.java");
    assertDocumentsTransactionAttribute("BatchStore.java");
    assertDocumentsTransactionAttribute("DlqAlertStore.java");
    assertDocumentsTransactionAttribute("JobBatchStatusStore.java");
    assertDocumentsTransactionAttribute("JobBulkStore.java");
    assertDocumentsTransactionAttribute("JobClaimStore.java");
    assertDocumentsTransactionAttribute("JobCrudStore.java");
    assertDocumentsTransactionAttribute("JobLogStore.java");
    assertDocumentsTransactionAttribute("JobPauseStore.java");
    assertDocumentsTransactionAttribute("JobRetryStore.java");
    assertDocumentsTransactionAttribute("JobTerminalStore.java");
    assertDocumentsTransactionAttribute("NodeStore.java");
    assertDocumentsTransactionAttribute("ResourcePermitStore.java");
    assertDocumentsTransactionAttribute("SignalStore.java");
    assertDocumentsTransactionAttribute("TagStore.java");
  }

  private static void assertDocumentsTransactionAttribute(String fileName) throws Exception {
    String source = Files.readString(SPI_ROOT.resolve(fileName));

    assertTrue(
        source.contains("Transaction attribute:"),
        () -> fileName + " should document store SPI transaction attributes");
  }
}
