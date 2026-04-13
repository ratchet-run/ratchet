package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;

/**
 * Tracks the set of {@code @Recurring} annotation keys this node knows about, plus the instant the
 * registration pass completed. Used by {@link RecurringJobExecutor} during a configurable startup
 * grace window to refuse firing recurring masters whose business key is not in the local
 * registration set.
 *
 * <p><b>Why this exists:</b> on a rolling deploy where Node B comes up with a JAR that has removed
 * an annotation, Node B's {@link run.ratchet.ri.cdi.RecurringJobProcessor} runs the
 * orphan-cleanup pass at startup. But the cleanup is leader-gated, so Node B may not be the one
 * running it — and even when it is, the {@link RecurringScheduler} polls the database independently
 * and may claim+fire the orphaned master BEFORE the cleanup pass completes. This grace check closes
 * that race: during the first {@code N} seconds after registration, the executor refuses to fire
 * any master whose business key is not in the local known set, regardless of leader state.
 *
 * <p>Configuration: {@code ratchet.recurring.startup-grace-seconds} system property, default 60.
 *
 * <p>The {@code RecurringRegistrationState} bean is application-scoped CDI, populated once when
 * {@link run.ratchet.ri.cdi.RecurringJobProcessor} finishes its startup pass and consulted
 * on every recurring scan thereafter. The known-keys set is concurrent so reads from the executor
 * thread don't need locking.
 */
@ApplicationScoped
public class RecurringRegistrationState {

  /** System property controlling the post-registration grace window in seconds. */
  public static final String STARTUP_GRACE_PROPERTY = "ratchet.recurring.startup-grace-seconds";

  private static final long DEFAULT_STARTUP_GRACE_SECONDS = 60L;

  private static final Logger log = Logger.getLogger(RecurringRegistrationState.class);

  private final Set<String> knownAnnotationKeys = ConcurrentHashMap.newKeySet();

  @SuppressWarnings("java:S3077")
  private volatile Instant registrationCompletedAt;

  /**
   * Records the keys discovered during a registration pass. Replaces any prior contents (so a
   * re-registration after a hot-swap is consistent). Sets the registration timestamp.
   *
   * @param keys business keys discovered for {@code @Recurring} annotated methods on this node
   */
  public void markRegistrationComplete(Set<String> keys) {
    knownAnnotationKeys.clear();
    knownAnnotationKeys.addAll(keys);
    registrationCompletedAt = Instant.now();
    log.debugf(
        "RecurringRegistrationState: marked %s known annotation key(s) at %s",
        keys.size(), registrationCompletedAt);
  }

  /**
   * Returns true if the given business key was registered on this node, OR if registration has not
   * yet completed (in which case we cannot make a safe judgment and default to permissive). After
   * the startup grace window expires this method always returns true: the cleanup pass has had a
   * chance to delete orphans, so any master still in the database is presumed live.
   *
   * @param businessKey the master's business key
   * @return true if the executor should be allowed to fire this master
   */
  public boolean shouldFire(String businessKey) {
    Instant completedAt = registrationCompletedAt;
    if (completedAt == null) {
      // Registration hasn't run yet — be permissive. This avoids breaking deployments that
      // don't use @Recurring at all (programmatic recurring jobs only). The
      // RecurringJobProcessor still calls markRegistrationComplete() with an empty set when
      // there are no annotations, so the grace window only applies once registration has
      // actually run.
      return true;
    }

    long graceSeconds = startupGraceSeconds();
    if (graceSeconds == 0) {
      return true;
    }

    Instant graceExpires = completedAt.plus(Duration.ofSeconds(graceSeconds));
    if (Instant.now().isAfter(graceExpires)) {
      return true;
    }

    // Within the grace window — only fire masters whose business key is in the local set.
    if (businessKey == null) {
      // Programmatically-submitted recurring jobs may not have a business key. They were not
      // declared via @Recurring, so the orphan cleanup never targets them — fire freely.
      return true;
    }

    boolean known = knownAnnotationKeys.contains(businessKey);
    if (!known) {
      log.debugf(
          "RecurringRegistrationState.shouldFire(%s) = false (within startup grace, key not in"
              + " local set of %s)",
          businessKey, knownAnnotationKeys.size());
    }
    return known;
  }

  /**
   * Returns true if the registration pass completed and we are still within the configured startup
   * grace window. Exposed for tests and diagnostics.
   */
  public boolean inStartupGrace() {
    Instant completedAt = registrationCompletedAt;
    if (completedAt == null) {
      return false;
    }
    long graceSeconds = startupGraceSeconds();
    if (graceSeconds == 0) {
      return false;
    }
    return Instant.now().isBefore(completedAt.plus(Duration.ofSeconds(graceSeconds)));
  }

  /**
   * Returns the registration completion instant, or {@code null} if registration has not completed.
   * Exposed for tests.
   */
  public Instant registrationCompletedAt() {
    return registrationCompletedAt;
  }

  /**
   * Reads {@link #STARTUP_GRACE_PROPERTY} from system properties on each call (not cached) so
   * operators can tune behavior via {@code -D} flags without rebuilding. Negative values are
   * clamped to zero (which disables the grace check).
   */
  public static long startupGraceSeconds() {
    String raw = System.getProperty(STARTUP_GRACE_PROPERTY);
    if (raw == null || raw.isBlank()) {
      return DEFAULT_STARTUP_GRACE_SECONDS;
    }
    try {
      long parsed = Long.parseLong(raw.trim());
      return Math.max(0L, parsed);
    } catch (NumberFormatException e) {
      log.warnf(
          "Invalid value for %s: '%s' — falling back to default %ss",
          STARTUP_GRACE_PROPERTY, raw, DEFAULT_STARTUP_GRACE_SECONDS);
      return DEFAULT_STARTUP_GRACE_SECONDS;
    }
  }

  /** Returns an unmodifiable snapshot of known annotation keys. Exposed for tests. */
  Set<String> snapshotKnownKeys() {
    return Collections.unmodifiableSet(knownAnnotationKeys);
  }

  /** Resets state. Intended for tests; production code should never need this. */
  void resetForTesting() {
    knownAnnotationKeys.clear();
    registrationCompletedAt = null;
  }
}
