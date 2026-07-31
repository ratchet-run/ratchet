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
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Spring-specific Ratchet bootstrap settings. */
@ConfigurationProperties(RatchetProperties.PREFIX)
public final class RatchetProperties {

  public static final String PREFIX = "ratchet";
  public static final String ENABLED_PROPERTY = "ratchet.enabled";
  public static final String TRANSACTION_MANAGER_BEAN_NAME_PROPERTY =
      "ratchet.transaction-manager-bean-name";
  public static final String LIFECYCLE_DEFER_AUTO_START_PROPERTY =
      "ratchet.lifecycle.defer-auto-start";
  public static final String LIFECYCLE_DRAIN_TIMEOUT_PROPERTY = "ratchet.lifecycle.drain-timeout";

  /** Whether Ratchet auto-configuration is enabled. */
  private boolean enabled = true;

  /** Bean name of the JpaTransactionManager Ratchet should use when multiple candidates exist. */
  private String transactionManagerBeanName;

  /** Runtime start and bounded-shutdown settings. */
  private Lifecycle lifecycle = new Lifecycle();

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getTransactionManagerBeanName() {
    return transactionManagerBeanName;
  }

  public void setTransactionManagerBeanName(String transactionManagerBeanName) {
    this.transactionManagerBeanName = transactionManagerBeanName;
  }

  public Lifecycle getLifecycle() {
    return lifecycle;
  }

  public void setLifecycle(Lifecycle lifecycle) {
    this.lifecycle = lifecycle;
  }

  /** Spring application lifecycle settings. */
  public static final class Lifecycle {

    /** Whether Spring should leave the Ratchet runtime stopped during context startup. */
    private boolean deferAutoStart;

    /** Maximum time to wait for in-flight jobs during context shutdown. */
    private Duration drainTimeout = Duration.ofSeconds(30);

    public boolean isDeferAutoStart() {
      return deferAutoStart;
    }

    public void setDeferAutoStart(boolean deferAutoStart) {
      this.deferAutoStart = deferAutoStart;
    }

    public Duration getDrainTimeout() {
      return drainTimeout;
    }

    public void setDrainTimeout(Duration drainTimeout) {
      this.drainTimeout = drainTimeout;
    }
  }
}
