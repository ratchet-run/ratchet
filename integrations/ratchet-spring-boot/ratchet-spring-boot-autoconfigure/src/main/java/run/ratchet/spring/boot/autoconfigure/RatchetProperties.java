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

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Spring-specific Ratchet bootstrap settings. */
@ConfigurationProperties(RatchetProperties.PREFIX)
public final class RatchetProperties {

  public static final String PREFIX = "ratchet";
  public static final String ENABLED_PROPERTY = "ratchet.enabled";
  public static final String TRANSACTION_MANAGER_BEAN_NAME_PROPERTY =
      "ratchet.transaction-manager-bean-name";

  /** Whether Ratchet auto-configuration is enabled. */
  private boolean enabled = true;

  /** Bean name of the JpaTransactionManager Ratchet should use when multiple candidates exist. */
  private String transactionManagerBeanName;

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
}
