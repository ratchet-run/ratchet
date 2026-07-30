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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.ProtectedSurface;

class EncryptionIntegrityTest {

  private static final UUID JOB = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

  @AfterEach
  void clear() {
    EncryptionIntegrity.clearListener();
  }

  @Test
  void flaggedButUnframed_incrementsTheCounterAndForwardsToTheListener() {
    List<ProtectedSurface> seen = new ArrayList<>();
    EncryptionIntegrity.setListener((jobId, surface) -> seen.add(surface));
    long before = EncryptionIntegrity.flaggedButUnframedCount();

    EncryptionIntegrity.flaggedButUnframed(JOB, ProtectedSurface.PAYLOAD_ARGS);

    assertEquals(before + 1, EncryptionIntegrity.flaggedButUnframedCount());
    assertEquals(List.of(ProtectedSurface.PAYLOAD_ARGS), seen);
  }

  @Test
  void clearListener_stopsForwardingButKeepsCounting() {
    List<ProtectedSurface> seen = new ArrayList<>();
    EncryptionIntegrity.setListener((jobId, surface) -> seen.add(surface));
    EncryptionIntegrity.clearListener();
    long before = EncryptionIntegrity.flaggedButUnframedCount();

    EncryptionIntegrity.flaggedButUnframed(JOB, ProtectedSurface.RESULT);

    assertEquals(before + 1, EncryptionIntegrity.flaggedButUnframedCount());
    assertTrue(seen.isEmpty());
  }

  @Test
  void tokenInstall_rejectsDifferentOwnerAndPreservesListener() {
    Object owner = new Object();
    List<String> seen = new ArrayList<>();
    EncryptionIntegrity.install(owner, (jobId, surface) -> seen.add("first"));

    assertThrows(
        IllegalStateException.class,
        () -> EncryptionIntegrity.install(new Object(), (jobId, surface) -> seen.add("second")));
    EncryptionIntegrity.flaggedButUnframed(JOB, ProtectedSurface.RESULT);

    assertEquals(List.of("first"), seen);
  }

  @Test
  void tokenInstall_sameOwnerMayReplaceListener() {
    Object owner = new Object();
    List<String> seen = new ArrayList<>();
    EncryptionIntegrity.install(owner, (jobId, surface) -> seen.add("first"));

    EncryptionIntegrity.install(owner, (jobId, surface) -> seen.add("second"));
    EncryptionIntegrity.flaggedButUnframed(JOB, ProtectedSurface.RESULT);

    assertEquals(List.of("second"), seen);
  }

  @Test
  void tokenUninstall_onlyClearsMatchingOwner() {
    Object owner = new Object();
    List<ProtectedSurface> seen = new ArrayList<>();
    EncryptionIntegrity.install(owner, (jobId, surface) -> seen.add(surface));

    EncryptionIntegrity.uninstall(new Object());
    EncryptionIntegrity.flaggedButUnframed(JOB, ProtectedSurface.RESULT);
    EncryptionIntegrity.uninstall(owner);
    EncryptionIntegrity.flaggedButUnframed(JOB, ProtectedSurface.PAYLOAD_ARGS);

    assertEquals(List.of(ProtectedSurface.RESULT), seen);
  }

  @Test
  void tokenInstall_allowsSequentialOwners() {
    Object firstOwner = new Object();
    Object secondOwner = new Object();
    List<String> seen = new ArrayList<>();

    EncryptionIntegrity.install(firstOwner, (jobId, surface) -> seen.add("first"));
    EncryptionIntegrity.uninstall(firstOwner);
    EncryptionIntegrity.install(secondOwner, (jobId, surface) -> seen.add("second"));
    EncryptionIntegrity.flaggedButUnframed(JOB, ProtectedSurface.RESULT);

    assertEquals(List.of("second"), seen);
  }

  @Test
  void legacySetListener_isAnonymousAndReplaceableByToken() {
    List<String> seen = new ArrayList<>();
    EncryptionIntegrity.setListener((jobId, surface) -> seen.add("legacy"));

    EncryptionIntegrity.install(new Object(), (jobId, surface) -> seen.add("owned"));
    EncryptionIntegrity.flaggedButUnframed(JOB, ProtectedSurface.RESULT);

    assertEquals(List.of("owned"), seen);
  }

  @Test
  void flaggedButUnframed_listenerFailureNeverPropagates() {
    EncryptionIntegrity.setListener(
        (jobId, surface) -> {
          throw new RuntimeException("metrics backend down");
        });
    long before = EncryptionIntegrity.flaggedButUnframedCount();

    // An operational signal must never break a read, even if the metrics bridge throws.
    EncryptionIntegrity.flaggedButUnframed(JOB, ProtectedSurface.PARAM_VALUE);

    assertEquals(before + 1, EncryptionIntegrity.flaggedButUnframedCount());
  }
}
