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

import static org.junit.jupiter.api.Assertions.fail;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
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
  @DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
  protected void cancelJob_rollback_isNotVisible() throws Exception {
    // The inherited assumeTrue guard fires in-container, where Arquillian wraps the
    // TestAbortedException in IdentifiedTestException and reports an ERROR instead of a skip.
    // Evaluate the same condition client-side so MongoDB cells skip cleanly.
    super.cancelJob_rollback_isNotVisible();
  }

  @Override
  @Test
  @Disabled(
      "Arquillian waits indefinitely for the remote servlet response on this inherited method")
  protected void pauseJob_rollback_doesNotSuppressExecution() {
    // Kept disabled because the inherited assertion hangs the Arquillian servlet round-trip in the
    // RI. The body fails loudly so removing @Disabled cannot pass vacuously — a future re-enable
    // must first restore a real, non-hanging contract body or fix the hang upstream.
    fail(
        "re-enable only after resolving the Arquillian servlet-response hang; do not run vacuously");
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create(AbstractTxRequiredContract.class.getPackage());
  }
}
