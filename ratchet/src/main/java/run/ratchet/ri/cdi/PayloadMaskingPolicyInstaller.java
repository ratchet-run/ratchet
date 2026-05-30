package run.ratchet.ri.cdi;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import run.ratchet.spi.PayloadMaskingPolicy;
import run.ratchet.store.util.PayloadMaskingPolicyHolder;

/**
 * Installs the framework-resolved {@link PayloadMaskingPolicy} into {@link
 * PayloadMaskingPolicyHolder} at application startup, and clears it at shutdown so a redeploy does
 * not leak a stale policy across the static holder.
 *
 * <p>{@link PayloadMasker} lives in {@code store-core} and may run outside a CDI container, so it
 * resolves its policy through the static holder rather than {@code @Inject}. This installer is the
 * bridge: a deployer that produces its own {@link PayloadMaskingPolicy} bean overrides the built-in
 * default; when no bean is produced the holder keeps returning the built-in policy and behavior is
 * unchanged.
 *
 * <p>Resolution is best-effort and null-safe: if no unambiguous policy bean is available the holder
 * is left on its built-in default.
 */
@ApplicationScoped
public class PayloadMaskingPolicyInstaller {

  private final Instance<PayloadMaskingPolicy> policy;

  @Inject
  public PayloadMaskingPolicyInstaller(Instance<PayloadMaskingPolicy> policy) {
    this.policy = policy;
  }

  void onStartup(@Observes @Initialized(ApplicationScoped.class) Object event) {
    if (policy != null && policy.isResolvable()) {
      PayloadMaskingPolicyHolder.set(policy.get());
    }
  }

  @PreDestroy
  void onShutdown() {
    PayloadMaskingPolicyHolder.set(null);
  }
}
