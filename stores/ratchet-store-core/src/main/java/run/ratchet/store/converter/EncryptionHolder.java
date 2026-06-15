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
package run.ratchet.store.converter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.PayloadEncryption;

/**
 * Static holder that resolves the active payload-encryption engines and key provider for the row
 * mappers and document mapper, which run outside any CDI container (raw unit tests, pre-deployment
 * tooling) and so cannot {@code @Inject} them.
 *
 * <p>Mirrors {@link PayloadSerializerHolder}: at container startup the reference implementation's
 * installer calls {@link #install} with the discovered engine beans and key provider; the
 * encryption walk consults {@link #isEnabled()} before doing any work, so a deployment with nothing
 * installed writes data byte-for-byte identical to one with no encryption configured at all.
 *
 * <p><b>Engine registry, not a single engine.</b> The holder keeps a map of engines keyed by {@link
 * PayloadEncryption#algorithmId()}. New writes use the designated <em>write</em> engine, but reads
 * dispatch on the algorithm id stored in the envelope, so a value written by an older engine stays
 * decryptable after the active engine is upgraded — old engines must remain installed until every
 * row that names them has drained.
 *
 * <p><b>Fail loud on misconfiguration.</b> {@link #install} is the single point where partial or
 * inconsistent wiring is rejected: a missing engine or provider, a blank or duplicate algorithm id.
 * The reference implementation's installer evaluates whether encryption was <em>requested</em> and
 * calls {@code install} only when it was, so a requested-but-unconfigured deployment fails at
 * startup rather than silently writing plaintext.
 */
public final class EncryptionHolder {

  private record State(
      Map<String, PayloadEncryption> engines,
      PayloadEncryption writeEngine,
      KeyProvider keyProvider,
      boolean enabled,
      boolean globalEnabled) {}

  private static final State DISABLED = new State(Map.of(), null, null, false, false);

  private static volatile State state = DISABLED;

  private EncryptionHolder() {}

  /**
   * Installs the active engines and key provider. Validates the wiring and rejects anything
   * inconsistent.
   *
   * @param engines the available encryption engines; must be non-empty
   * @param writeAlgorithmId the algorithm id of the engine used for new writes; must name one of
   *     {@code engines}
   * @param keyProvider the key provider; must not be {@code null}
   * @param globalEnabled whether the deployment-wide encryption switch is on; when {@code true}
   *     every job's surfaces are encrypted, when {@code false} only jobs that opt in are
   * @throws EncryptionConfigurationException if {@code engines} is empty, {@code keyProvider} is
   *     {@code null}, an engine reports a blank algorithm id, two engines report the same id, or
   *     {@code writeAlgorithmId} names no installed engine
   */
  public static void install(
      Collection<PayloadEncryption> engines,
      String writeAlgorithmId,
      KeyProvider keyProvider,
      boolean globalEnabled) {
    if (engines == null || engines.isEmpty()) {
      throw new EncryptionConfigurationException(
          "Payload encryption is enabled but no PayloadEncryption engine is installed");
    }
    if (keyProvider == null) {
      throw new EncryptionConfigurationException(
          "Payload encryption is enabled but no KeyProvider is installed");
    }
    Map<String, PayloadEncryption> registry = new HashMap<>();
    for (PayloadEncryption engine : engines) {
      String id = engine.algorithmId();
      if (id == null || id.isBlank()) {
        throw new EncryptionConfigurationException(
            "PayloadEncryption " + engine.getClass().getName() + " returned a blank algorithmId");
      }
      if (registry.putIfAbsent(id, engine) != null) {
        throw new EncryptionConfigurationException(
            "Two PayloadEncryption engines report the same algorithmId: " + id);
      }
    }
    PayloadEncryption write = registry.get(writeAlgorithmId);
    if (write == null) {
      throw new EncryptionConfigurationException(
          "Configured write algorithm is not installed: " + writeAlgorithmId);
    }
    state = new State(Map.copyOf(registry), write, keyProvider, true, globalEnabled);
  }

  /** Reverts to the disabled state. Called at container shutdown and between tests. */
  public static void disable() {
    state = DISABLED;
  }

  /**
   * Returns {@code true} when an engine and key provider are installed. The read path uses this to
   * know whether decryption is even possible; write-side callers use {@link
   * #encryptionActiveFor(boolean)} instead, which also applies the global and per-job switches.
   */
  public static boolean isEnabled() {
    return state.enabled;
  }

  /**
   * Decides whether a write must be encrypted, given whether the owning job opted in.
   *
   * <p>Encryption is wanted when the deployment-wide switch is on or the job opted in. When it is
   * wanted but no engine/provider is installed, this fails loud rather than silently writing
   * plaintext for a value the caller asked to protect — the runtime mirror of the installer's
   * startup check.
   *
   * @param jobOptedIn whether the owning job opted in via {@code withEncryptedPayload()}
   * @return {@code true} if the value must be encrypted, {@code false} to store it as plaintext
   * @throws EncryptionConfigurationException if encryption is wanted but not configured
   */
  public static boolean encryptionActiveFor(boolean jobOptedIn) {
    State current = state;
    boolean wanted = current.globalEnabled || jobOptedIn;
    if (!wanted) {
      return false;
    }
    if (!current.enabled) {
      throw new EncryptionConfigurationException(
          "A job requested payload encryption but no PayloadEncryption engine and KeyProvider are"
              + " installed");
    }
    return true;
  }

  /** Returns {@code true} when the deployment-wide encryption switch is on. */
  public static boolean isGloballyEnabled() {
    return state.globalEnabled;
  }

  /**
   * Returns the engine used for new writes (the current/active algorithm).
   *
   * @throws IllegalStateException if encryption is not enabled
   */
  public static PayloadEncryption writeEngine() {
    State current = state;
    if (!current.enabled) {
      throw new IllegalStateException("Encryption is not enabled");
    }
    return current.writeEngine;
  }

  /**
   * Resolves the engine for a stored value's algorithm id (read-time dispatch).
   *
   * @param algorithmId the algorithm id read from a stored envelope
   * @return the engine that implements {@code algorithmId}
   * @throws PayloadDecryptionException if no installed engine implements the algorithm — poison
   *     data, not retryable; the remediation is to re-install the engine and replay
   */
  public static PayloadEncryption engine(String algorithmId) {
    PayloadEncryption engine = state.engines.get(algorithmId);
    if (engine == null) {
      throw new PayloadDecryptionException(
          "No PayloadEncryption engine installed for algorithm: " + algorithmId);
    }
    return engine;
  }

  /**
   * Returns the active key provider.
   *
   * @throws IllegalStateException if encryption is not enabled
   */
  public static KeyProvider keyProvider() {
    State current = state;
    if (!current.enabled) {
      throw new IllegalStateException("Encryption is not enabled");
    }
    return current.keyProvider;
  }
}
