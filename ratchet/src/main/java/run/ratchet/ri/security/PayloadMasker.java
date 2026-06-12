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
package run.ratchet.ri.security;

import java.util.Map;
import run.ratchet.spi.MaskingContext;

/**
 * Public entry point for masking sensitive fields (passwords, tokens, PII) in job payload data
 * before it is rendered into a log line or returned from a read API. Which fields are sensitive is
 * decided by the active {@link run.ratchet.spi.PayloadMaskingPolicy}; deployers can override the
 * default field set by producing their own policy. The original payload in the database is never
 * modified.
 */
public final class PayloadMasker {

  private PayloadMasker() {}

  /** Masks sensitive fields in a job payload JSON string; returns null if input is null. */
  public static String maskPayload(String payloadJson) {
    return run.ratchet.store.util.PayloadMasker.maskPayload(payloadJson);
  }

  /**
   * Context-aware variant of {@link #maskPayload(String)}: the policy's context-aware overload is
   * consulted per field. A {@code null} context falls back to name-only matching.
   */
  public static String maskPayload(String payloadJson, MaskingContext context) {
    return run.ratchet.store.util.PayloadMasker.maskPayload(payloadJson, context);
  }

  /**
   * Serializes {@code payload} to JSON then masks sensitive fields; returns null if input is null.
   */
  public static String maskPayload(Object payload) {
    return run.ratchet.store.util.PayloadMasker.maskPayload(payload);
  }

  /** Context-aware variant of {@link #maskPayload(Object)}. */
  public static String maskPayload(Object payload, MaskingContext context) {
    return run.ratchet.store.util.PayloadMasker.maskPayload(payload, context);
  }

  /**
   * Masks the values of sensitive entries in a parameter map; a {@code null} or empty map is
   * returned unchanged. Keys are matched against the active policy, so the original map is never
   * modified.
   */
  public static Map<String, String> maskParams(Map<String, String> params) {
    return run.ratchet.store.util.PayloadMasker.maskParams(params);
  }

  /** Context-aware variant of {@link #maskParams(Map)}. */
  public static Map<String, String> maskParams(Map<String, String> params, MaskingContext context) {
    return run.ratchet.store.util.PayloadMasker.maskParams(params, context);
  }
}
