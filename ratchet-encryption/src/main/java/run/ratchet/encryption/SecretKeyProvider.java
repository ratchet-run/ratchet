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
package run.ratchet.encryption;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.api.exception.KeyNotFoundException;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.LocalEncryptionKey;

/**
 * A reference {@link KeyProvider} backed by one or more in-process AES-256 secret keys supplied by
 * the deployment (an environment variable, a secrets file, a config value). It is the simplest
 * provider that supports key rotation: it holds a map of keys by id and a designated
 * <em>current</em> key.
 *
 * <p><b>Rotation.</b> {@link #currentKey()} returns the current key — new writes use it, and its id
 * is stamped into each envelope. {@link #keyById(String)} resolves any still-installed key, so rows
 * written under an older key stay decryptable until they drain. To rotate, add a new key, point the
 * current id at it, and keep the old keys installed until no row references them. A reference to a
 * key that has been removed is {@link KeyNotFoundException} — poison, not retryable.
 *
 * <p><b>Not for external key services.</b> This provider exposes raw key material in-process; a KMS
 * or HSM that never releases key bytes is a {@code WrappedKeyProvider} instead. Key lifecycle
 * (zeroization) is the deployment's: the provider holds the supplied {@link SecretKey} instances
 * for the life of the process.
 *
 * <p>Immutable after construction and therefore thread-safe.
 */
public final class SecretKeyProvider implements KeyProvider {

  private static final int AES_256_KEY_BYTES = 32;

  private final Map<String, LocalEncryptionKey> keysById;
  private final LocalEncryptionKey currentKey;

  /**
   * @param keysById the installed keys by id; must be non-empty and every key must be a 256-bit AES
   *     key
   * @param currentKeyId the id of the key new writes use; must name an entry in {@code keysById}
   * @throws EncryptionConfigurationException if the map is empty, a key is not 256-bit, or {@code
   *     currentKeyId} names no installed key
   */
  public SecretKeyProvider(Map<String, SecretKey> keysById, String currentKeyId) {
    if (keysById == null || keysById.isEmpty()) {
      throw new EncryptionConfigurationException("SecretKeyProvider requires at least one key");
    }
    if (currentKeyId == null || currentKeyId.isBlank()) {
      throw new EncryptionConfigurationException("SecretKeyProvider requires a current key id");
    }
    Map<String, LocalEncryptionKey> built = new LinkedHashMap<>();
    for (Map.Entry<String, SecretKey> entry : keysById.entrySet()) {
      String keyId = entry.getKey();
      SecretKey material = entry.getValue();
      if (keyId == null || keyId.isBlank()) {
        throw new EncryptionConfigurationException("SecretKeyProvider key id must not be blank");
      }
      validateAes256(keyId, material);
      built.put(keyId, new StaticLocalKey(keyId, material));
    }
    LocalEncryptionKey current = built.get(currentKeyId);
    if (current == null) {
      throw new EncryptionConfigurationException(
          "SecretKeyProvider current key id is not installed: " + currentKeyId);
    }
    this.keysById = Map.copyOf(built);
    this.currentKey = current;
  }

  /**
   * Builds a provider from base64-encoded key material — the shape a deployment supplies through an
   * environment variable or config value.
   *
   * @param base64KeysById each value is the base64 (standard or URL-safe) encoding of a 256-bit key
   * @param currentKeyId the id of the current key
   * @return a configured provider
   * @throws EncryptionConfigurationException if a value is not valid base64 or not a 256-bit key
   */
  public static SecretKeyProvider fromBase64(
      Map<String, String> base64KeysById, String currentKeyId) {
    if (base64KeysById == null || base64KeysById.isEmpty()) {
      throw new EncryptionConfigurationException("SecretKeyProvider requires at least one key");
    }
    Map<String, SecretKey> keys = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : base64KeysById.entrySet()) {
      keys.put(entry.getKey(), decodeAesKey(entry.getKey(), entry.getValue()));
    }
    return new SecretKeyProvider(keys, currentKeyId);
  }

  @Override
  public EncryptionKey currentKey() {
    return currentKey;
  }

  @Override
  public EncryptionKey keyById(String keyId) {
    LocalEncryptionKey key = keysById.get(keyId);
    if (key == null) {
      throw new KeyNotFoundException("No key installed for id: " + keyId);
    }
    return key;
  }

  private static SecretKey decodeAesKey(String keyId, String base64) {
    if (base64 == null || base64.isBlank()) {
      throw new EncryptionConfigurationException("SecretKeyProvider key '" + keyId + "' is blank");
    }
    byte[] raw;
    try {
      raw = Base64.getDecoder().decode(base64.trim());
    } catch (IllegalArgumentException tryUrlSafe) {
      try {
        raw = Base64.getUrlDecoder().decode(base64.trim());
      } catch (IllegalArgumentException e) {
        throw new EncryptionConfigurationException(
            "SecretKeyProvider key '" + keyId + "' is not valid base64", e);
      }
    }
    SecretKey key = new SecretKeySpec(raw, "AES");
    validateAes256(keyId, key);
    return key;
  }

  private static void validateAes256(String keyId, SecretKey material) {
    if (material == null) {
      throw new EncryptionConfigurationException("SecretKeyProvider key '" + keyId + "' is null");
    }
    byte[] encoded = material.getEncoded();
    if (encoded == null || encoded.length != AES_256_KEY_BYTES) {
      throw new EncryptionConfigurationException(
          "SecretKeyProvider key '"
              + keyId
              + "' must be a 256-bit (32-byte) AES key, but was "
              + (encoded == null ? "non-extractable" : encoded.length + " bytes"));
    }
  }

  private record StaticLocalKey(String keyId, SecretKey material) implements LocalEncryptionKey {}
}
