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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import run.ratchet.api.KeyRange;
import run.ratchet.ri.cdi.internal.JsonbPayloadSerializer;
import run.ratchet.ri.payload.ArgumentMaterializer;
import run.ratchet.ri.security.PackagePrefixClassPolicy;
import run.ratchet.store.entity.JobPayload;

class PartitionPayloadTest {
  @Test
  void recordRoundTripsAndMaterializesWithDefaultPolicy() {
    var serializer = new JsonbPayloadSerializer();
    var range = new KeyRange(-5, Long.MAX_VALUE);
    assertEquals(range, serializer.deserialize(serializer.serialize(range), KeyRange.class));
    var policy = new PackagePrefixClassPolicy(Set.of("run.ratchet.ri.core"));
    assertTrue(policy.isAllowed(KeyRange.class.getName()));
    var payload =
        new JobPayload(
            PartitionPayloadTest.class.getName(),
            "process",
            "(Lrun/ratchet/api/KeyRange;)V",
            true,
            List.of(Map.of("from", -5L, "to", Long.MAX_VALUE)),
            null);
    assertEquals(
        List.of(range), ArgumentMaterializer.materialize(payload, null, serializer, policy).args());
  }

  public static void process(KeyRange range) {}
}
