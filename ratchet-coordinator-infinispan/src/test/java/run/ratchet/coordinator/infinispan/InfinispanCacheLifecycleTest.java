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
package run.ratchet.coordinator.infinispan;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.infinispan.Cache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;

class InfinispanCacheLifecycleTest {

  @SuppressWarnings("unchecked")
  private final Cache<String, String> cache = mock(Cache.class);

  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();
  private final InfinispanCoordinatorConfig config =
      new InfinispanCoordinatorConfig("wakeup", Optional.empty(), 60L, 16_384, 2, 500L);

  @BeforeEach
  void setUp() {
    when(cache.addListenerAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(cache.removeListenerAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
  }

  @Test
  void startRegistersListener() {
    InfinispanCacheLifecycle lifecycle = newLifecycle();
    lifecycle.start();
    verify(cache).addListenerAsync(any(InfinispanWakeupListener.class));
  }

  @Test
  void publishCallsPutAsyncWithConfiguredTtl() {
    InfinispanCacheLifecycle lifecycle = newLifecycle();
    lifecycle.publish("key1", "value1");
    verify(cache).putAsync("key1", "value1", 60L, TimeUnit.SECONDS);
  }

  @Test
  void publishHonoursConfiguredTtlOverride() {
    InfinispanCoordinatorConfig overridden =
        new InfinispanCoordinatorConfig("wakeup", Optional.empty(), 5L, 16_384, 2, 500L);
    InfinispanCacheLifecycle lifecycle =
        new InfinispanCacheLifecycle(cache, overridden, codec, m -> {}, () -> {});
    lifecycle.publish("k", "v");
    verify(cache).putAsync("k", "v", 5L, TimeUnit.SECONDS);
  }

  @Test
  void closeRemovesListener() {
    InfinispanCacheLifecycle lifecycle = newLifecycle();
    lifecycle.start();
    lifecycle.close();
    verify(cache).removeListenerAsync(any(InfinispanWakeupListener.class));
  }

  @Test
  void closeIsIdempotent() {
    InfinispanCacheLifecycle lifecycle = newLifecycle();
    lifecycle.start();
    lifecycle.close();
    assertDoesNotThrow(lifecycle::close);
    // removeListenerAsync should fire only once.
    verify(cache, times(1)).removeListenerAsync(any(InfinispanWakeupListener.class));
  }

  @Test
  void closeWithoutStartIsNoOp() {
    InfinispanCacheLifecycle lifecycle = newLifecycle();
    assertDoesNotThrow(lifecycle::close);
    verify(cache, times(0)).removeListenerAsync(any(InfinispanWakeupListener.class));
  }

  @Test
  void isClosedTracksLifecycleState() {
    InfinispanCacheLifecycle lifecycle = newLifecycle();
    lifecycle.start();
    assertFalse(lifecycle.isClosed());
    lifecycle.close();
    assertTrue(lifecycle.isClosed());
  }

  @Test
  void startAfterCloseIsNoOp() {
    InfinispanCacheLifecycle lifecycle = newLifecycle();
    lifecycle.close();
    lifecycle.start();
    verify(cache, times(0)).addListenerAsync(any(InfinispanWakeupListener.class));
  }

  private InfinispanCacheLifecycle newLifecycle() {
    return new InfinispanCacheLifecycle(cache, config, codec, m -> {}, () -> {});
  }
}
