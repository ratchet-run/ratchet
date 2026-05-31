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
import run.ratchet.tck.jakarta.AbstractTxSupportsContract;

/**
 * RI subclass of {@link AbstractTxSupportsContract}. The RI builder factory methods do not write to
 * the store; their terminal {@code submit()} delegates to the same JPA path as {@code enqueueNow},
 * which participates in the JTA transaction.
 */
@ExtendWith(ArquillianExtension.class)
class RiTxSupportsIT extends AbstractTxSupportsContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  @Test
  @DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
  protected void enqueueSubmit_insideRolledBackTx_jobDoesNotExecute() throws Exception {
    super.enqueueSubmit_insideRolledBackTx_jobDoesNotExecute();
  }

  @Override
  @Test
  @DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
  protected void scheduleSubmit_insideRolledBackTx_jobDoesNotExecute() throws Exception {
    super.scheduleSubmit_insideRolledBackTx_jobDoesNotExecute();
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create(AbstractTxSupportsContract.class.getPackage());
  }
}
