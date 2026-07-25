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
package run.ratchet.quarkus.it.tck;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Optional;
import run.ratchet.tck.api.AbstractRetryPolicyContract;
import run.ratchet.tck.api.RatchetTckRuntime;

/** Quarkus binding for {@link AbstractRetryPolicyContract}. */
@QuarkusTest
@TestProfile(QuarkusRetryPolicyProfile.class)
class QuarkusRetryPolicyTckTest extends AbstractRetryPolicyContract {

  @Inject QuarkusRatchetTckRuntime runtime;

  @Override
  protected Optional<RatchetTckRuntime> retryPolicyRuntime() {
    return Optional.of(runtime);
  }
}
