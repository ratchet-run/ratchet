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
    assertDocumentsTransactionAttribute("BatchStore.java");
    assertDocumentsTransactionAttribute("DlqAlertStore.java");
    assertDocumentsTransactionAttributes("JobAnalyticsStore.java", 21);
    assertDocumentsTransactionAttributes("JobAuditStore.java", 6);
    assertDocumentsTransactionAttribute("JobBatchStatusStore.java");
    assertDocumentsTransactionAttribute("JobBulkStore.java");
    assertDocumentsTransactionAttribute("JobClaimStore.java");
    assertDocumentsTransactionAttribute("JobCrudStore.java");
    assertDocumentsTransactionAttribute("JobPauseStore.java");
    assertDocumentsTransactionAttributes("JobQueryStore.java", 3);
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
