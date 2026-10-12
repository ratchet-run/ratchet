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
package run.ratchet.testsuite.core;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.interceptor.Interceptor;
import java.util.Optional;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import run.ratchet.api.internal.RatchetConfigKeys;
import run.ratchet.spi.RatchetConfigSource;
import run.ratchet.testsuite.app.TestRuntimeConfig;

/** Enables a cooperative cancellation grace period for this deployment only. */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.APPLICATION + 100)
public class CooperativeCancellationOptionsProducer {
  @Produces
  @ApplicationScoped
  public RatchetOptions ratchetOptions() {
    return RatchetOptionsFactory.fromEnvironment(new GraceOverride(), new TestRuntimeConfig());
  }

  private static final class GraceOverride implements RatchetConfigSource {
    @Override
    public Optional<String> get(String propertyName, String environmentVariable) {
      return RatchetConfigKeys.CANCELLATION_GRACE_SECONDS.name().equals(propertyName)
          ? Optional.of("3")
          : Optional.empty();
    }
  }
}
