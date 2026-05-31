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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.jakarta.AbstractTxRequiredContract;

/**
 * RI subclass of {@link AbstractTxRequiredContract}. The RI's JPA-backed stores write within the
 * JTA-managed RatchetDS transaction, so mutations committed or rolled back by the caller are
 * durable or invisible accordingly.
 */
@ExtendWith(ArquillianExtension.class)
class RiTxRequiredIT extends AbstractTxRequiredContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  @Test
  @Disabled(
      "Arquillian waits indefinitely for the remote servlet response on this inherited method")
  protected void pauseJob_rollback_doesNotSuppressExecution() {
    // Disabled explicitly so the unsupported RI contract is visible in test reports.
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create(AbstractTxRequiredContract.class.getPackage());
  }
}
