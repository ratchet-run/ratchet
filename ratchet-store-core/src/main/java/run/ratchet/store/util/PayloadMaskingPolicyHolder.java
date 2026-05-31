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

  private static volatile PayloadMaskingPolicy delegate;

  private PayloadMaskingPolicyHolder() {}

  /**
   * Installs the framework-managed {@link PayloadMaskingPolicy}. Called once at container startup
   * by the reference implementation's producer.
   *
   * @param policy the policy to install; MAY be {@code null} to revert to the built-in default
   */
  public static void set(PayloadMaskingPolicy policy) {
    delegate = policy;
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
