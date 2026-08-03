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

import run.ratchet.spi.PayloadMaskingPolicy;

/**
 * Static holder that resolves the active {@link PayloadMaskingPolicy} used by {@link
 * PayloadMasker}.
 *
 * <p>Mirrors {@link run.ratchet.store.converter.PayloadSerializerHolder}: {@link PayloadMasker}
 * lives in {@code store-core} and may run outside a CDI container (raw unit tests, pre-deployment
 * tooling), so it cannot {@code @Inject} the policy. At container startup the reference
 * implementation's producer calls {@link #set(PayloadMaskingPolicy)} with the discovered CDI bean.
 * When no policy has been installed the holder returns {@link DefaultPayloadMaskingPolicy}, so the
 * default field set masks identically with or without a container.
 */
public final class PayloadMaskingPolicyHolder {

  private static final PayloadMaskingPolicy DEFAULT = new DefaultPayloadMaskingPolicy();
  private static final String OWNER_CONFLICT_MESSAGE =
      "Payload masking policy is already installed by a different owner";

  private static volatile PayloadMaskingPolicy delegate;

  private static final OwnerTokenGuard OWNER = new OwnerTokenGuard();

  private PayloadMaskingPolicyHolder() {}

  /**
   * Installs the framework-managed {@link PayloadMaskingPolicy}. Called once at container startup
   * by the reference implementation's producer.
   *
   * @param policy the policy to install; MAY be {@code null} to revert to the built-in default
   */
  public static synchronized void set(PayloadMaskingPolicy policy) {
    OWNER.reset();
    delegate = policy;
  }

  /**
   * Installs the framework-managed policy for a specific runtime owner.
   *
   * <p>Ownership is compared by identity. Re-installing with the same token is idempotent and may
   * replace the policy. An installation made through {@link #set(PayloadMaskingPolicy)} is
   * anonymous and may be replaced by any token.
   *
   * @param ownerToken the non-null identity token for the installing runtime
   * @param policy the policy to install
   * @throws IllegalStateException if another token currently owns the holder
   */
  public static synchronized void install(Object ownerToken, PayloadMaskingPolicy policy) {
    OWNER.claim(ownerToken, OWNER_CONFLICT_MESSAGE);
    delegate = policy;
  }

  /**
   * Clears the policy only when {@code ownerToken} is the token that installed it.
   *
   * <p>A different token, or an anonymously installed policy, is left unchanged.
   *
   * @param ownerToken the non-null identity token for the uninstalling runtime
   */
  public static synchronized void uninstall(Object ownerToken) {
    if (OWNER.release(ownerToken)) {
      delegate = null;
    }
  }

  /**
   * Returns the currently-installed {@link PayloadMaskingPolicy}, or the built-in {@link
   * DefaultPayloadMaskingPolicy} if none has been registered.
   */
  public static PayloadMaskingPolicy get() {
    PayloadMaskingPolicy current = delegate;
    return current != null ? current : DEFAULT;
  }
}
