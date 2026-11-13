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
import java.util.concurrent.ScheduledExecutorService;

/** Relays committed cancellations to attempts running on this node. */
@ApplicationScoped
public class CancelRequestRelay {
  static final Duration TICK_INTERVAL = Duration.ofSeconds(5);

  protected CancelRequestRelay() {}

  @Inject
  public CancelRequestRelay(
      JobTimeoutHandler timeoutHandler,
      CancelStatusReader statusReader,
      InternalEventPublisher eventPublisher,
      Clock clock) {}

  public synchronized void start(ScheduledExecutorService executor) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  public synchronized void stop() {
    throw new UnsupportedOperationException("not implemented yet");
  }

  void tick() {
    throw new UnsupportedOperationException("not implemented yet");
  }
}
