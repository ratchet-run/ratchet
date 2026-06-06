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
import java.util.Optional;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.AbstractRetryPolicyContract;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.testsuite.app.SecondAttemptVetoRetryPolicy;

/**
 * RI subclass of {@link AbstractRetryPolicyContract}. The deployment bundles {@link
 * SecondAttemptVetoRetryPolicy} as an enabled {@code @Alternative}, so the whole engine in this
 * deployment retries with that policy.
 */
@ExtendWith(ArquillianExtension.class)
class RiRetryPolicyIT extends AbstractRetryPolicyContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected Optional<RatchetTckRuntime> retryPolicyRuntime() {
    return Optional.of(runtime);
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.createWith(new Package[0], SecondAttemptVetoRetryPolicy.class);
  }
}
