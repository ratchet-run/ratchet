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
package run.ratchet.store.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonParsingException;
import java.io.StringReader;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.spi.WrappedKeyProvider;
import run.ratchet.store.converter.EncryptionHolder;

/**
 * Applies the active {@link PayloadEncryption} engine to sensitive payload values, keeping the
 * surrounding JSON envelope valid. Modeled on {@link PayloadMasker}: it walks serialized JSON with
 * JSON-P and transforms selected leaves rather than whole columns, so structural routing metadata
 * ({@code target}, {@code method}) stays in cleartext and the indexed generated columns derived
 * from it keep working.
 *
 * <p><b>Framing and binding.</b> This class — not the engine — owns the {@link EncryptionEnvelope}
 * and the {@link EncryptionAad} computation. Each protected leaf is encrypted under the active
 * engine and key, wrapped in a versioned {@code rcph:e} frame, and bound to its {@link
 * EncryptionTarget} so a ciphertext lifted into another row, surface, or signal fails the AEAD tag.
 * On read, only values carrying a complete frame are decrypted; everything else passes through, so
 * encrypted and legacy-plaintext rows coexist during a rollout.
 *
 * <p><b>The gate.</b> Every write-side method takes an {@code active} flag — the caller's single
 * decision, computed once via {@link EncryptionHolder#encryptionActiveFor(boolean)} so the same
 * value drives both the encryption and the row's {@code encrypted_payload}/{@code
 * encryption_key_id} columns. When {@code active} is false the input is returned unchanged with no
 * JSON walk, so an unencrypted deployment stores byte-for-byte identical data. Read-side methods
 * are driven purely by the frame marker, so they remain correct regardless of the gate.
 */
public final class PayloadEncryptor {

  private static final String ARGS = "args";
  private static final byte[] NO_WRAPPED_KEY = new byte[0];

  private PayloadEncryptor() {}

  // ---- Single values (signal payload in TEXT; an individual parameter value) ----

  /**
   * Encrypts a single value into a frame, or returns it unchanged when encryption is not active for
   * the owning job. The result is a bare token suitable for a TEXT column or string field.
   */
  public static String encryptValue(String value, boolean active, EncryptionTarget target) {
    if (value == null || !active) {
      return value;
    }
    return encryptToFrame(value.getBytes(UTF_8), target);
  }

  /** Reverses {@link #encryptValue}; values without the frame marker are returned unchanged. */
  public static String decryptValue(String value, EncryptionTarget target) {
    EncryptionEnvelope.Frame frame = EncryptionEnvelope.decode(value);
    return frame == null ? value : new String(decryptFrame(frame, target), UTF_8);
  }

  // ---- Whole JSON/JSONB column (job result) ----

  /**
   * Encrypts a whole serialized JSON document into a valid-JSON envelope: a single JSON string
   * literal wrapping the frame. Suitable for a {@code JSON}/{@code JSONB} column. Returns the input
   * unchanged when encryption is not active for the owning job.
   */
  public static String encryptJsonColumn(String json, boolean active, EncryptionTarget target) {
    if (json == null || json.isEmpty() || !active) {
      return json;
    }
    return Json.createValue(encryptToFrame(json.getBytes(UTF_8), target)).toString();
  }

  /**
   * Reverses {@link #encryptJsonColumn}. A stored value that is not a frame-carrying JSON string
   * (legacy plaintext JSON, or a genuine string value) is returned unchanged.
   */
  public static String decryptJsonColumn(String stored, EncryptionTarget target) {
    if (stored == null || stored.isEmpty()) {
      return stored;
    }
    JsonValue value = tryParse(stored);
    if (value == null || value.getValueType() != JsonValue.ValueType.STRING) {
      return stored;
    }
    EncryptionEnvelope.Frame frame = EncryptionEnvelope.decode(((JsonString) value).getString());
    return frame == null ? stored : new String(decryptFrame(frame, target), UTF_8);
  }

  // ---- Job payload (encrypt the args sub-tree; leave routing metadata cleartext) ----

  /**
   * Encrypts the {@code args} sub-tree of a serialized payload, leaving {@code target}, {@code
   * method}, {@code methodDescriptor}, and {@code isStatic} in cleartext so the generated columns
   * derived from them keep working. Returns the input unchanged when encryption is not active for
   * the owning job or the payload carries no arguments.
   */
  public static String encryptArgs(String payloadJson, boolean active, EncryptionTarget target) {
    if (payloadJson == null || payloadJson.isEmpty() || !active) {
      return payloadJson;
    }
    try (JsonReader reader = Json.createReader(new StringReader(payloadJson))) {
      JsonValue root = reader.readValue();
      if (root.getValueType() != JsonValue.ValueType.OBJECT) {
        return payloadJson;
      }
      JsonObject object = root.asJsonObject();
      JsonValue args = object.get(ARGS);
      if (args == null || args.getValueType() == JsonValue.ValueType.NULL) {
        return payloadJson;
      }
      String token = encryptToFrame(args.toString().getBytes(UTF_8), target);
      return replaceArgs(object, Json.createValue(token));
    }
  }

  /**
   * Reports whether a row whose {@code encrypted_payload} flag is set nonetheless stored an
   * unframed {@code args} subtree — a write-time integrity anomaly (a plaintext downgrade, a node
   * without the engine active, or a bug). Returns {@code false} for a payload that legitimately
   * carries no {@code args} to protect. Marker-driven and allocation-light: it parses the envelope
   * JSON but never decrypts.
   *
   * <p><b>Scope: systemic downgrade only.</b> The {@code args} subtree is the probe for a
   * <em>systemic</em> downgrade — a node without the engine active, a flipped global switch, or a
   * write-path bug on the shared gate — because every protected surface on a row resolves the same
   * per-row {@code active} decision ({@link EncryptionHolder#encryptionActiveFor(boolean)}), and
   * {@code args} is present on every job row. It does <em>not</em> detect a
   * <em>partial-surface</em> downgrade where {@code args} is framed but {@code job_result}, a
   * parameter value, or a signal payload was stored as plaintext: those surfaces are decrypted
   * marker-driven on read but are not independently integrity-probed. That gap is bounded by the
   * single-gate design (a per-surface divergence requires a surface computing {@code active}
   * differently from the row flag, not a routine misconfiguration); widening the probe to those
   * surfaces is tracked as future work. See {@link run.ratchet.store.util.EncryptionIntegrity}.
   */
  public static boolean argsFlaggedButUnframed(String payloadJson) {
    if (payloadJson == null || payloadJson.isEmpty()) {
      return false;
    }
    JsonValue root = tryParse(payloadJson);
    if (root == null || root.getValueType() != JsonValue.ValueType.OBJECT) {
      return false;
    }
    JsonValue args = root.asJsonObject().get(ARGS);
    if (args == null || args.getValueType() == JsonValue.ValueType.NULL) {
      return false;
    }
    // Encrypted args are a single framed string; anything else under a set flag (an array/object,
    // or
    // a string without the frame marker) is plaintext the flag promised would be ciphertext.
    return args.getValueType() != JsonValue.ValueType.STRING
        || !EncryptionEnvelope.isFramed(((JsonString) args).getString());
  }

  /** Reverses {@link #encryptArgs}, restoring the {@code args} value in place. */
  public static String decryptArgs(String payloadJson, EncryptionTarget target) {
    if (payloadJson == null || payloadJson.isEmpty()) {
      return payloadJson;
    }
    JsonValue root = tryParse(payloadJson);
    if (root == null || root.getValueType() != JsonValue.ValueType.OBJECT) {
      return payloadJson;
    }
    JsonObject object = root.asJsonObject();
    JsonValue args = object.get(ARGS);
    if (args == null || args.getValueType() != JsonValue.ValueType.STRING) {
      return payloadJson;
    }
    EncryptionEnvelope.Frame frame = EncryptionEnvelope.decode(((JsonString) args).getString());
    if (frame == null) {
      return payloadJson;
    }
    JsonValue restored = parse(new String(decryptFrame(frame, target), UTF_8));
    return replaceArgs(object, restored);
  }

  // ---- Parameter map (a JSON object of string values; encrypt each value) ----

  /**
   * Encrypts every string value of a serialized JSON object (a parameter map), leaving keys in
   * cleartext. Returns the input unchanged when encryption is not active for the owning job.
   */
  public static String encryptParamMap(String mapJson, boolean active, EncryptionTarget target) {
    if (mapJson == null || mapJson.isEmpty() || !active) {
      return mapJson;
    }
    return rebuildMap(mapJson, target, true);
  }

  /** Reverses {@link #encryptParamMap}; values without the marker are left unchanged. */
  public static String decryptParamMap(String mapJson, EncryptionTarget target) {
    if (mapJson == null || mapJson.isEmpty()) {
      return mapJson;
    }
    return rebuildMap(mapJson, target, false);
  }

  // ---- Internals ----

  private static String encryptToFrame(byte[] plaintext, EncryptionTarget target) {
    PayloadEncryption engine = EncryptionHolder.writeEngine();
    KeyProvider provider = EncryptionHolder.keyProvider();
    EncryptionKey key;
    byte[] wrappedKey;
    if (provider instanceof WrappedKeyProvider wrappingProvider) {
      // Envelope encryption: a fresh DEK encrypts the value and its wrapped form is persisted in
      // the
      // (authenticated) envelope wrapped-key field so a later read can recover the DEK.
      WrappedKeyProvider.WrappedKey wrapped = wrappingProvider.currentWrappedKey();
      key = wrapped.key();
      wrappedKey = wrapped.wrapped();
    } else {
      key = provider.currentKey();
      wrappedKey = NO_WRAPPED_KEY;
    }
    byte[] header =
        EncryptionEnvelope.canonicalHeader(engine.algorithmId(), key.keyId(), wrappedKey);
    byte[] aad = EncryptionAad.compute(header, target.surface(), target.binding());
    byte[] body =
        engine.encrypt(
            plaintext, new EncryptionContext(target.surface(), target.jobId(), key, aad));
    return EncryptionEnvelope.encode(header, body);
  }

  private static byte[] decryptFrame(EncryptionEnvelope.Frame frame, EncryptionTarget target) {
    // engine() throws PayloadDecryptionException for an unknown/uninstalled algorithm (poison),
    // which also covers a node that holds ciphertext but has no engine configured.
    PayloadEncryption engine = EncryptionHolder.engine(frame.algorithmId());
    KeyProvider provider = EncryptionHolder.keyProvider();
    EncryptionKey key;
    if (frame.wrappedKey().length > 0 && provider instanceof WrappedKeyProvider wrappingProvider) {
      // The value was written under envelope encryption: recover the DEK by unwrapping under the
      // master key named by keyId. A wrapped value cannot be resolved by id alone.
      key = wrappingProvider.unwrapKey(frame.keyId(), frame.wrappedKey());
    } else {
      key = provider.keyById(frame.keyId());
    }
    byte[] aad = EncryptionAad.compute(frame.canonicalHeader(), target.surface(), target.binding());
    return engine.decrypt(
        frame.body(), new EncryptionContext(target.surface(), target.jobId(), key, aad));
  }

  private static String replaceArgs(JsonObject object, JsonValue newArgs) {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    for (var entry : object.entrySet()) {
      builder.add(entry.getKey(), ARGS.equals(entry.getKey()) ? newArgs : entry.getValue());
    }
    return builder.build().toString();
  }

  private static String rebuildMap(String mapJson, EncryptionTarget target, boolean encrypt) {
    JsonValue root = tryParse(mapJson);
    if (root == null || root.getValueType() != JsonValue.ValueType.OBJECT) {
      return mapJson;
    }
    JsonObject object = root.asJsonObject();
    JsonObjectBuilder builder = Json.createObjectBuilder();
    for (var entry : object.entrySet()) {
      JsonValue value = entry.getValue();
      if (value.getValueType() == JsonValue.ValueType.STRING) {
        String raw = ((JsonString) value).getString();
        String transformed =
            encrypt ? encryptToFrame(raw.getBytes(UTF_8), target) : decryptStringValue(raw, target);
        builder.add(entry.getKey(), transformed);
      } else {
        builder.add(entry.getKey(), value);
      }
    }
    return builder.build().toString();
  }

  private static String decryptStringValue(String raw, EncryptionTarget target) {
    EncryptionEnvelope.Frame frame = EncryptionEnvelope.decode(raw);
    return frame == null ? raw : new String(decryptFrame(frame, target), UTF_8);
  }

  private static JsonValue parse(String json) {
    try (JsonReader reader = Json.createReader(new StringReader(json))) {
      return reader.readValue();
    }
  }

  /**
   * Parses {@code json}, returning {@code null} for input that is not valid JSON (no token to act
   * on), so the decrypt path leaves malformed/legacy values for the downstream serializer to
   * report.
   */
  private static JsonValue tryParse(String json) {
    try {
      return parse(json);
    } catch (JsonParsingException e) {
      return null;
    }
  }
}
