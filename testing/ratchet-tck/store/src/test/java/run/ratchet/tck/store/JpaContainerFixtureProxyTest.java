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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.store.spi.JobPropertyStore;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStore;

class JpaContainerFixtureProxyTest {

  @Test
  void proxyImplementsOnlyTheCapabilitiesTheDelegateAdvertises() {
    assertEquals(List.of(JobStore.class), interfacesFor(mock(JobStore.class, CALLS_REAL_METHODS)));
    assertEquals(
        List.of(JobStore.class, JobPropertyStore.class),
        interfacesFor(storeWith(JobPropertyStore.class)));
    assertEquals(
        List.of(JobStore.class, JobStateStore.class),
        interfacesFor(storeWith(JobStateStore.class)));
  }

  private static JobStore storeWith(Class<?> capability) {
    return mock(
        JobStore.class,
        withSettings().extraInterfaces(capability).defaultAnswer(CALLS_REAL_METHODS));
  }

  private static List<Class<?>> interfacesFor(JobStore delegate) {
    return List.of(JpaContainerFixture.proxyInterfaces(delegate));
  }
}
