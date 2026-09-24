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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.spi.JobStore;

class RatchetEncryptionConfigurationTest {
  @Test
  void multipleEnginesRequireExplicitWriteAlgorithm() {
    PayloadEncryption first = mock(PayloadEncryption.class);
    PayloadEncryption second = mock(PayloadEncryption.class);
    when(first.algorithmId()).thenReturn("first");
    when(second.algorithmId()).thenReturn("second");
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class))
        .withPropertyValues("ratchet.allowed-packages=example.jobs")
        .withBean("first", PayloadEncryption.class, () -> first)
        .withBean("second", PayloadEncryption.class, () -> second)
        .withBean(KeyProvider.class, () -> mock(KeyProvider.class))
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(context.getStartupFailure())
                  .hasRootCauseInstanceOf(EncryptionConfigurationException.class)
                  .hasRootCauseMessage(
                      "Multiple PayloadEncryption engines are installed but no write algorithm is"
                          + " configured. Set RatchetOptions.encryption().writeAlgorithm to the"
                          + " algorithm id new writes should use.");
            });
    assertThat(EncryptionHolder.isEnabled()).isFalse();
  }

  @Test
  void customEncryptionPairTakesPrecedenceOverReferenceProperties() {
    PayloadEncryption engine = mock(PayloadEncryption.class);
    when(engine.algorithmId()).thenReturn("custom");
    KeyProvider keys = mock(KeyProvider.class);
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class))
        .withPropertyValues(
            "ratchet.allowed-packages=example.jobs",
            "ratchet.encryption.enabled=true",
            "ratchet.encryption.write-algorithm=custom",
            "ratchet.encryption.keys=invalid-reference-configuration-is-unused")
        .withBean(PayloadEncryption.class, () -> engine)
        .withBean(KeyProvider.class, () -> keys)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(EncryptionHolder.engine("custom")).isSameAs(engine);
              assertThat(EncryptionHolder.keyProvider()).isSameAs(keys);
              assertThat(EncryptionHolder.isGloballyEnabled()).isTrue();
            });
  }

  @Test
  void partialCustomStackIsRejectedInsteadOfMixingInReferenceComponents() {
    String key = Base64.getEncoder().encodeToString(new byte[32]);
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class))
        .withPropertyValues(
            "ratchet.allowed-packages=example.jobs", "ratchet.encryption.keys=test:" + key)
        .withBean(KeyProvider.class, () -> mock(KeyProvider.class))
        .run(context -> assertThat(context).hasFailed());
    assertThat(EncryptionHolder.isEnabled()).isFalse();
  }

  @Test
  void existingEncryptionPropertiesUseTheSpringEnvironmentAndReleaseOnClose() {
    String key = Base64.getEncoder().encodeToString(new byte[32]);
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RatchetAutoConfiguration.class, RatchetEngineAutoConfiguration.class))
        .withPropertyValues(
            "ratchet.allowed-packages=example.jobs",
            "ratchet.encryption.enabled=true",
            "ratchet.encryption.keys=test:" + key)
        .withBean(JobStore.class, () -> mock(JobStore.class))
        .withBean(RatchetRuntime.class, () -> mock(RatchetRuntime.class))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(EncryptionHolder.isGloballyEnabled()).isTrue();
              assertThat(EncryptionHolder.encryptionActiveFor(false)).isTrue();
            });
    assertThat(EncryptionHolder.isEnabled()).isFalse();
  }
}
