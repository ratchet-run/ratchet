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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.logging.Logger;
import run.ratchet.spi.ProtectedSurface;

/**
 * Read-path integrity probe for the ADR's Q-D decision: it counts and (throttled-)logs rows whose
 * {@code encrypted_payload} flag is set but whose stored value reads back as unframed plaintext.
 *
 * <p>This is an operational-integrity <em>signal</em>, not a security control. The flag is a
 * cleartext column, so an attacker who could strip framing from a value could also clear the flag;
 * the divergence is therefore evidence of a write-time fault, not of tampering. Reads are never
 * failed on this condition — flagged-plaintext rows are legitimately produced during an enable
 * rollout, before every node has the engine active — but surfacing the divergence makes a
 * misconfiguration, an un-upgraded node, or a write-side bug visible instead of silent.
 *
 * <p>The row mappers and document mapper that observe the condition run outside any CDI container,
 * so they reach the metrics layer through the static {@link Listener} a container-side bridge
 * installs at startup, mirroring {@link EncryptionHolder}. With no listener installed the counter
 * and the throttled log still work.
 */
public final class EncryptionIntegrity {

  /** Receives each flagged-but-unframed observation so a container can forward it to metrics. */
  @FunctionalInterface
  public interface Listener {
    void onFlaggedButUnframed(UUID jobId, ProtectedSurface surface);
  }

  private static final Logger log = Logger.getLogger(EncryptionIntegrity.class);
  private static final long WARN_INTERVAL_MS = 60_000L;

  private static final AtomicLong flaggedButUnframed = new AtomicLong();
  private static volatile long lastWarnAtMs;
  private static volatile Listener listener;

  private EncryptionIntegrity() {}

  /** Installs the metrics bridge. Called at container startup; replaced or cleared on shutdown. */
  public static void setListener(Listener bridge) {
    listener = bridge;
  }

  /** Removes the metrics bridge. Called at container shutdown and between tests. */
  public static void clearListener() {
    listener = null;
  }

  /** Total flagged-but-unframed reads observed since the process started. */
  public static long flaggedButUnframedCount() {
    return flaggedButUnframed.get();
  }

  /**
   * Records one flagged-but-unframed read: increments the counter, logs at most once per minute,
   * and forwards to the metrics {@link Listener} when one is installed. Never throws.
   *
   * @param jobId the affected job (or recurring master)
   * @param surface the protected surface that read back unframed
   */
  public static void flaggedButUnframed(UUID jobId, ProtectedSurface surface) {
    long total = flaggedButUnframed.incrementAndGet();
    maybeWarn(jobId, surface, total);
    Listener current = listener;
    if (current != null) {
      try {
        current.onFlaggedButUnframed(jobId, surface);
      } catch (RuntimeException e) {
        log.debugf(e, "Encryption-integrity metrics listener failed for job %s", jobId);
      }
    }
  }

  private static void maybeWarn(UUID jobId, ProtectedSurface surface, long total) {
    long now = System.currentTimeMillis();
    if (now - lastWarnAtMs < WARN_INTERVAL_MS) {
      return;
    }
    // Best-effort throttle: a rare duplicate log line under a race is acceptable for an ops signal.
    lastWarnAtMs = now;
    log.warnf(
        "Job %s surface %s is flagged encrypted but read back as unframed plaintext (%d total since"
            + " startup) — a write-time downgrade, an un-upgraded node, or a bug. Reads are not"
            + " failed; investigate the writer.",
        jobId, surface, total);
  }
}
