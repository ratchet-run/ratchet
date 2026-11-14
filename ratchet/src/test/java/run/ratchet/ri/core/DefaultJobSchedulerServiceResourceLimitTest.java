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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ResourceLimit;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.store.spi.BatchStore;

class DefaultJobSchedulerServiceResourceLimitTest {

  private final ResourcePermitService permits = mock(ResourcePermitService.class);
  private final DefaultJobSchedulerService scheduler = scheduler(permits);

  @Test
  void setResourceLimitStoresTheNormalizedLimitThroughThePermitService() {
    scheduler.setResourceLimit(
        new ResourceLimit("  payment-gateway ", 3, Duration.ofMillis(1500), " External API "));

    // The permit service, not the store, takes the write, so it wakes waiters after commit.
    verify(permits).configureResource("payment-gateway", 3, 1500, "External API");
  }

  @Test
  void setResourceLimitUsesTheDefaultsOfOf() {
    scheduler.setResourceLimit(ResourceLimit.of("gpu", 2));

    verify(permits).configureResource("gpu", 2, 5000, null);
  }

  @Test
  void setResourceLimitDropsAnyFractionOfAMillisecond() {
    scheduler.setResourceLimit(
        ResourceLimit.of("gpu", 1).withRetryDelay(Duration.ofNanos(1_999_999)));

    verify(permits).configureResource("gpu", 1, 1, null);
  }

  @Test
  void setResourceLimitRejectsInvalidArgumentsBeforeAnyStoreCall() {
    assertEquals(
        "Resource limit must not be null",
        assertThrows(IllegalArgumentException.class, () -> scheduler.setResourceLimit(null))
            .getMessage());
    assertThrows(
        IllegalArgumentException.class,
        () -> scheduler.setResourceLimit(ResourceLimit.of("gpu", 0)));
    assertThrows(
        IllegalArgumentException.class,
        () -> scheduler.setResourceLimit(ResourceLimit.of("r".repeat(101), 1)));
    verifyNoInteractions(permits);
  }

  @Test
  void setResourceLimitReportsAnInvalidArgumentEvenWithoutThePermitCapability() {
    // Argument errors are the caller's bug whatever the store, so they win over the capability
    // error, which only the permit service can raise.
    doThrow(new UnsupportedOperationException("no capability"))
        .when(permits)
        .configureResource("gpu", 1, 5000, null);

    assertThrows(
        IllegalArgumentException.class,
        () -> scheduler.setResourceLimit(ResourceLimit.of("gpu", 0)));
    assertEquals(
        "no capability",
        assertThrows(
                UnsupportedOperationException.class,
                () -> scheduler.setResourceLimit(ResourceLimit.of("gpu", 1)))
            .getMessage());
  }

  @Test
  void findResourceLimitTrimsTheNameAndReturnsWhatThePermitServiceFinds() {
    ResourceLimit stored = new ResourceLimit("gpu", 2, Duration.ofMillis(100), null);
    when(permits.findResourceLimit("gpu")).thenReturn(Optional.of(stored));

    assertEquals(Optional.of(stored), scheduler.findResourceLimit("  gpu "));
  }

  @Test
  void findResourceLimitReturnsEmptyForNullOrBlankWithoutAStoreCall() {
    assertEquals(Optional.empty(), scheduler.findResourceLimit(null));
    assertEquals(Optional.empty(), scheduler.findResourceLimit(" \t"));
    verifyNoInteractions(permits);
  }

  @Test
  void findResourceLimitRejectsANameNoLimitCanHave() {
    assertThrows(
        IllegalArgumentException.class, () -> scheduler.findResourceLimit("r".repeat(101)));
    assertThrows(IllegalArgumentException.class, () -> scheduler.findResourceLimit("gpü"));
    verifyNoInteractions(permits);
  }

  @Test
  void listResourceLimitsReturnsWhatThePermitServiceLists() {
    List<ResourceLimit> limits = List.of(ResourceLimit.of("api", 1), ResourceLimit.of("gpu", 2));
    when(permits.listResourceLimits()).thenReturn(limits);

    assertSame(limits, scheduler.listResourceLimits());
  }

  private static DefaultJobSchedulerService scheduler(ResourcePermitService permits) {
    return new DefaultJobSchedulerService(
        null,
        null,
        null,
        null,
        null,
        null,
        (BatchStore) null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        Clock.systemUTC(),
        null,
        new JakartaAfterCommitRegistrar(),
        null,
        permits);
  }
}
