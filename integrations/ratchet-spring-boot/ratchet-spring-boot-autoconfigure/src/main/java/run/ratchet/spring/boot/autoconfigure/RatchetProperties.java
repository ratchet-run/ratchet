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
package run.ratchet.spring.boot.autoconfigure;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Options specific to the Spring integration; engine options retain their existing names. */
@ConfigurationProperties("ratchet")
public class RatchetProperties {
  /** Enables migrations, recurring discovery, and the scheduler runtime. */
  private boolean enabled = true;

  /** Maximum time to drain running jobs before cancellation during shutdown. */
  private Duration shutdownTimeout = Duration.ofSeconds(30);

  /** Invocation packages. When absent, uses the Boot application's auto-configuration packages. */
  private Set<String> allowedPackages;

  /** Additional packages whose classes may be materialized from persisted results. */
  private Set<String> allowedResultTypePackages = new LinkedHashSet<>();

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Duration getShutdownTimeout() {
    return shutdownTimeout;
  }

  public void setShutdownTimeout(Duration value) {
    if (value == null || value.isNegative())
      throw new IllegalArgumentException("ratchet.shutdown-timeout must be non-negative");
    shutdownTimeout = value;
  }

  public Set<String> getAllowedPackages() {
    return allowedPackages;
  }

  public void setAllowedPackages(Set<String> allowedPackages) {
    this.allowedPackages = allowedPackages;
  }

  public Set<String> getAllowedResultTypePackages() {
    return allowedResultTypePackages;
  }

  public void setAllowedResultTypePackages(Set<String> value) {
    allowedResultTypePackages = value;
  }
}
