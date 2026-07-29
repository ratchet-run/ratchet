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
package run.ratchet.quarkus.runtime.devui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.inject.Instance;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;

class RatchetDevUiJsonRPCServiceTest {

  @Test
  void snapshotReportsUnavailableQueryServices() {
    var service = new RatchetDevUiJsonRPCService();
    service.jobQueryService = unavailableInstance();
    service.clusterQueryService = unavailableInstance();

    RatchetSnapshot snapshot = service.getSnapshot();

    assertTrue(snapshot.jobs().isEmpty());
    assertTrue(snapshot.nodes().isEmpty());
    assertEquals(QueueHealthRow.empty(), snapshot.health());
    assertEquals(
        "Job query service is unavailable. Queue health is unavailable. "
            + "Cluster query service is unavailable.",
        snapshot.status());
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> unavailableInstance() {
    return (Instance<T>)
        Proxy.newProxyInstance(
            RatchetDevUiJsonRPCServiceTest.class.getClassLoader(),
            new Class<?>[] {Instance.class},
            (proxy, method, arguments) -> {
              if (method.getName().equals("isResolvable")) {
                return false;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
