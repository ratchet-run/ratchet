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
package run.ratchet.spring.boot.it.mysql.tck;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import run.ratchet.spring.boot.it.mysql.fixture.tck.SpringRatchetTckRuntime;
import run.ratchet.spring.boot.it.mysql.fixture.tck.SpringRatchetTransactionDriver;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;
import run.ratchet.tck.api.transaction.AbstractTxNotSupportedContract;
import run.ratchet.tck.api.transaction.RatchetTransactionDriver;

/** Spring MySQL binding for {@link AbstractTxNotSupportedContract}. */
@SpringMysqlTck
class SpringTxNotSupportedTckTest extends AbstractTxNotSupportedContract {

  @Autowired SpringRatchetTckRuntime runtime;
  @Autowired SpringRatchetTransactionDriver transactionDriver;

  @BeforeEach
  void clearBeforeEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  protected RatchetTransactionDriver transactionDriver() {
    return transactionDriver;
  }
}
