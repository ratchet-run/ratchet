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
package run.ratchet.showcase.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class OrderRepositoryTest {

  @Test
  void startAfterStopContinuesOrderSequenceUntilReset() {
    OrderRepository repository = new OrderRepository();
    repository.startStream(1L, 30, 0.25, 0.35);
    assertEquals(1L, repository.nextSequence());

    repository.stopStream();
    repository.startStream(2L, 30, 0.25, 0.35);
    assertEquals(2L, repository.nextSequence());

    repository.reset();
    repository.startStream(3L, 30, 0.25, 0.35);
    assertEquals(1L, repository.nextSequence());
  }

  @Test
  void seedUpdateDoesNotReuseOrderSequence() {
    OrderRepository repository = new OrderRepository();
    repository.startStream(1L, 30, 0.25, 0.35);
    assertEquals(1L, repository.nextSequence());

    repository.updateStream(null, 99L, null, null);

    assertEquals(2L, repository.nextSequence());
  }
}
