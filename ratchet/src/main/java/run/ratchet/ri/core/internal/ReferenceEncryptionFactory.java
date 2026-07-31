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
package run.ratchet.ri.core.internal;

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

/** Builds the bundled reference encryption stack from deployment configuration. */
public final class ReferenceEncryptionFactory {

  public static final String KEYS_ENV = "RATCHET_ENCRYPTION_KEYS";
  public static final String CURRENT_KEY_ENV = "RATCHET_ENCRYPTION_CURRENT_KEY";
  public static final String KEYS_PROPERTY = "ratchet.encryption.keys";
  public static final String CURRENT_KEY_PROPERTY = "ratchet.encryption.current-key";

  /** The bundled reference engine and key provider, ready to install. */
  public record ReferenceEncryption(PayloadEncryption engine, KeyProvider keyProvider) {}

  private ReferenceEncryptionFactory() {}

  /** Resolves the reference stack from environment variables and system properties. */
  public static Optional<ReferenceEncryption> fromEnvironment(long nodeEntropy) {
    return build(
        resolve(KEYS_ENV, KEYS_PROPERTY),
        resolve(CURRENT_KEY_ENV, CURRENT_KEY_PROPERTY),
        nodeEntropy);
  }

  /** Builds the reference stack from already-resolved configuration strings. */
  public static Optional<ReferenceEncryption> build(
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
      int separator = trimmed.indexOf(':');
      if (separator <= 0 || separator == trimmed.length() - 1) {
        throw new EncryptionConfigurationException(
            "RATCHET_ENCRYPTION_KEYS entry must be 'keyId:base64Key', got: " + trimmed);
      }
      String keyId = trimmed.substring(0, separator).trim();
      String base64 = trimmed.substring(separator + 1).trim();
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

  private static String resolve(String environmentName, String propertyName) {
    String fromEnvironment = System.getenv(environmentName);
    if (fromEnvironment != null && !fromEnvironment.isBlank()) {
      return fromEnvironment;
    }
    return System.getProperty(propertyName);
  }

  /** Returns a stable 64-bit FNV-1a hash of the node id. */
  public static long nodeEntropy(String nodeId) {
    if (nodeId == null || nodeId.isEmpty()) {
      return 0L;
    }
    long hash = 0xcbf29ce484222325L;
    for (byte value : nodeId.getBytes(StandardCharsets.UTF_8)) {
      hash ^= (value & 0xff);
      hash *= 0x100000001b3L;
    }
    return hash;
  }
}
