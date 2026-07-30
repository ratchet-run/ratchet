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
package run.ratchet.spring.boot.it.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionTemplate;

class SpringTransactionDriverContractTest {

  @Test
  void committingReturnsSupplierValueAndCommitsWork() {
    SpringBootManagedBeanCompatibilityTest.RecordingTransactionManager transactionManager =
        new SpringBootManagedBeanCompatibilityTest.RecordingTransactionManager();
    SpringTransactionDriver driver =
        new SpringTransactionDriver(new TransactionTemplate(transactionManager));
    AtomicBoolean workRan = new AtomicBoolean();

    String result =
        driver.committing(
            () -> {
              workRan.set(true);
              return "committed";
            });

    assertEquals("committed", result);
    assertTrue(workRan.get());
    assertEquals(List.of("BEGIN(REQUIRED)", "COMMIT"), transactionManager.events());
  }

  @Test
  void rollingBackReturnsSupplierValueAndRollsBackWork() {
    SpringBootManagedBeanCompatibilityTest.RecordingTransactionManager transactionManager =
        new SpringBootManagedBeanCompatibilityTest.RecordingTransactionManager();
    SpringTransactionDriver driver =
        new SpringTransactionDriver(new TransactionTemplate(transactionManager));
    AtomicBoolean workRan = new AtomicBoolean();

    String result =
        driver.rollingBack(
            () -> {
              workRan.set(true);
              return "rolled back";
            });

    assertEquals("rolled back", result);
    assertTrue(workRan.get());
    assertEquals(List.of("BEGIN(REQUIRED)", "ROLLBACK"), transactionManager.events());
  }
}
