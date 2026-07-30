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
package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.transaction.RatchetTransactionDriver;
import run.ratchet.tck.jakarta.AbstractTxRequiresNewContract;

/**
 * RI runner for the portable post-execution {@code REQUIRES_NEW} contract. MongoDB does not enlist
 * scheduler writes in the caller's JTA transaction, so its rollback case is capability-exempt.
 */
@ExtendWith(ArquillianExtension.class)
class RiTxRequiresNewIT extends AbstractTxRequiresNewContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  @Test
  @DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
  protected void completedState_survivesCallerRollback() throws Exception {
    super.completedState_survivesCallerRollback();
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create(
        AbstractTxRequiresNewContract.class.getPackage(),
        RatchetTransactionDriver.class.getPackage());
  }
}
