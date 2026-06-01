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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;

/**
 * Tracks {@code @Recurring} annotation keys this node discovered at startup. During a configurable
 * grace window, the executor refuses to fire masters whose key is not in the local set, closing a
 * race between orphan cleanup and the recurring poller on rolling deploys.
 */
@ApplicationScoped
public class RecurringRegistrationState {

  private static final Logger log = Logger.getLogger(RecurringRegistrationState.class);

  private final long startupGraceSeconds;
  private final Clock clock;

  @SuppressWarnings("java:S3077")
  private volatile Set<String> knownAnnotationKeys = Set.of();

  @SuppressWarnings("java:S3077")
  private volatile Instant registrationCompletedAt;

  public RecurringRegistrationState() {
    this(RatchetOptions.defaults());
  }

  public RecurringRegistrationState(RatchetOptions options) {
    this(options, Clock.systemUTC());
  }

  @Inject
  public RecurringRegistrationState(RatchetOptions options, Clock clock) {
    this.startupGraceSeconds = options.recurring().startupGraceSeconds();
    this.clock = clock != null ? clock : Clock.systemUTC();
  }

  /**
   * Records the keys discovered during a registration pass. Replaces any prior contents (so a
   * re-registration after a hot-swap is consistent). Sets the registration timestamp.
   *
   * @param keys business keys discovered for {@code @Recurring} annotated methods on this node
   */
  public void markRegistrationComplete(Set<String> keys) {
    boolean reregistration = registrationCompletedAt != null;
    Set<String> snapshot = Set.copyOf(keys);
    knownAnnotationKeys = snapshot;
    registrationCompletedAt = Instant.now(clock);
    if (reregistration && snapshot.isEmpty()) {
      log.warn(
          "Recurring annotation re-registration completed with no keys; annotation-created masters"
              + " will be held during the startup grace window");
      return;
    }
    log.debugf(
        "RecurringRegistrationState: marked %s known annotation key(s) at %s",
        snapshot.size(), registrationCompletedAt);
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

    long graceSeconds = startupGraceSeconds;
    if (graceSeconds == 0) {
      return true;
    }

    Instant graceExpires = completedAt.plus(Duration.ofSeconds(graceSeconds));
    if (Instant.now(clock).isAfter(graceExpires)) {
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

  public boolean inStartupGrace() {
    Instant completedAt = registrationCompletedAt;
    if (completedAt == null) {
      return false;
    }
    long graceSeconds = startupGraceSeconds;
    if (graceSeconds == 0) {
      return false;
    }
    return Instant.now(clock).isBefore(completedAt.plus(Duration.ofSeconds(graceSeconds)));
  }

  public Instant registrationCompletedAt() {
    return registrationCompletedAt;
  }

  public long startupGraceSeconds() {
    return startupGraceSeconds;
  }

  Set<String> snapshotKnownKeys() {
    return Collections.unmodifiableSet(knownAnnotationKeys);
  }

  void resetForTesting() {
    knownAnnotationKeys = Set.of();
    registrationCompletedAt = null;
  }
}
