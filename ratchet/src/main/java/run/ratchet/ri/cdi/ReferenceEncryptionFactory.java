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
package run.ratchet.ri.cdi;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.encryption.AesGcmPayloadEncryption;
import run.ratchet.encryption.SecretKeyProvider;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.PayloadEncryption;

/**
 * Builds the bundled reference encryption stack — the AES-256-GCM engine plus a {@link
 * SecretKeyProvider} — from deployment configuration, so a deployment can turn on encryption with
 * no code by supplying keys through the environment. It activates only when no application has
 * provided its own {@code PayloadEncryption}/{@code KeyProvider} beans; an app that brings its own
 * takes precedence (see {@link EncryptionInstaller}).
 *
 * <p><b>Configuration.</b> Key material is read from {@code RATCHET_ENCRYPTION_KEYS} (or system
 * property {@code ratchet.encryption.keys}) as a comma-separated list of {@code keyId:base64Key}
 * entries, and the current write key from {@code RATCHET_ENCRYPTION_CURRENT_KEY} (or {@code
 * ratchet.encryption.current-key}). With a single key the current id may be omitted. Keys are kept
 * out of {@link run.ratchet.api.RatchetOptions} deliberately — secret material belongs in the
 * environment, not the in-memory options object.
 */
final class ReferenceEncryptionFactory {

  static final String KEYS_ENV = "RATCHET_ENCRYPTION_KEYS";
  static final String CURRENT_KEY_ENV = "RATCHET_ENCRYPTION_CURRENT_KEY";
  static final String KEYS_PROPERTY = "ratchet.encryption.keys";
  static final String CURRENT_KEY_PROPERTY = "ratchet.encryption.current-key";

  /** The bundled reference engine and key provider, ready to install. */
  record ReferenceEncryption(PayloadEncryption engine, KeyProvider keyProvider) {}

  private ReferenceEncryptionFactory() {}

  /**
   * Resolves the reference stack from the environment and system properties, or empty when no keys
   * are configured.
   *
   * @param nodeEntropy per-node entropy mixed into the engine's nonce epoch (clone-safety)
   */
  static Optional<ReferenceEncryption> fromEnvironment(long nodeEntropy) {
    return build(
        resolve(KEYS_ENV, KEYS_PROPERTY),
        resolve(CURRENT_KEY_ENV, CURRENT_KEY_PROPERTY),
        nodeEntropy);
  }

  /**
   * Builds the reference stack from already-resolved configuration strings, or empty when {@code
   * keysSpec} is blank. Package-visible for direct unit testing without touching the environment.
   *
   * @param keysSpec comma-separated {@code keyId:base64Key} entries, or {@code null}/blank
   * @param currentKeyId the current write key id, or {@code null} to default to the sole key
   * @param nodeEntropy per-node entropy mixed into the engine's nonce epoch
   */
  static Optional<ReferenceEncryption> build(
      String keysSpec, String currentKeyId, long nodeEntropy) {
    if (keysSpec == null || keysSpec.isBlank()) {
      return Optional.empty();
    }
    Map<String, String> base64KeysById = parseKeys(keysSpec);
    String current = resolveCurrentKeyId(currentKeyId, base64KeysById);
    SecretKeyProvider provider = SecretKeyProvider.fromBase64(base64KeysById, current);
    PayloadEncryption engine = new AesGcmPayloadEncryption(new SecureRandom(), nodeEntropy);
    return Optional.of(new ReferenceEncryption(engine, provider));
  }

  private static Map<String, String> parseKeys(String keysSpec) {
    Map<String, String> keys = new LinkedHashMap<>();
    for (String entry : keysSpec.split(",")) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      int sep = trimmed.indexOf(':');
      if (sep <= 0 || sep == trimmed.length() - 1) {
        throw new EncryptionConfigurationException(
            "RATCHET_ENCRYPTION_KEYS entry must be 'keyId:base64Key', got: " + trimmed);
      }
      String keyId = trimmed.substring(0, sep).trim();
      String base64 = trimmed.substring(sep + 1).trim();
      if (keys.put(keyId, base64) != null) {
        throw new EncryptionConfigurationException(
            "RATCHET_ENCRYPTION_KEYS contains duplicate key id: " + keyId);
      }
    }
    if (keys.isEmpty()) {
      throw new EncryptionConfigurationException("RATCHET_ENCRYPTION_KEYS configured but empty");
    }
    return keys;
  }

  private static String resolveCurrentKeyId(String configured, Map<String, String> keys) {
    if (configured != null && !configured.isBlank()) {
      return configured.trim();
    }
    if (keys.size() == 1) {
      return keys.keySet().iterator().next();
    }
    throw new EncryptionConfigurationException(
        "Multiple encryption keys configured but RATCHET_ENCRYPTION_CURRENT_KEY is not set");
  }

  private static String resolve(String envName, String propertyName) {
    String fromEnv = System.getenv(envName);
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv;
    }
    return System.getProperty(propertyName);
  }

  /**
   * A stable 64-bit hash (FNV-1a) of the node id, mixed into the engine's nonce epoch so two nodes
   * sharing a key produce disjoint nonce spaces even if their RNGs were seeded identically.
   */
  static long nodeEntropy(String nodeId) {
    if (nodeId == null || nodeId.isEmpty()) {
      return 0L;
    }
    long hash = 0xcbf29ce484222325L;
    for (byte b : nodeId.getBytes(StandardCharsets.UTF_8)) {
      hash ^= (b & 0xff);
      hash *= 0x100000001b3L;
    }
    return hash;
  }
}
