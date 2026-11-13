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
package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.api.internal.DefaultRatchetConfig;
import run.ratchet.api.internal.RatchetConfigKeys;
import run.ratchet.spi.RatchetConfigSource;

class PartitionOptionsTest {
  @Test
  void canonicalKeysBindThroughTheSharedFactory() {
    Map<String, String> values =
        Map.of("ratchet.batch.max-partitions", "23", "ratchet.batch.partition-grace", "PT2M");
    var options =
        RatchetOptionsFactory.from(
            new DefaultRatchetConfig(
                List.<RatchetConfigSource>of(
                    (property, environment) -> Optional.ofNullable(values.get(property)))));
    assertEquals(23, options.maintenance().batchMaxPartitions());
    assertEquals(Duration.ofMinutes(2), options.maintenance().batchPartitionGrace());
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetConfigKeys.BATCH_MAX_PARTITIONS.parser().apply("0"));
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetConfigKeys.BATCH_PARTITION_GRACE.parser().apply("PT0S"));
  }

  @Test
  void defaultsAndCopy() {
    var options = RatchetOptions.builder().build();
    assertEquals(10000, options.maintenance().batchMaxPartitions());
    assertEquals(Duration.ofMinutes(30), options.maintenance().batchPartitionGrace());
    var configured =
        options.toBuilder()
            .maintenance(m -> m.batchMaxPartitions(7).batchPartitionGrace(Duration.ofSeconds(9)))
            .build();
    assertEquals(7, configured.toBuilder().build().maintenance().batchMaxPartitions());
    assertEquals(
        Duration.ofSeconds(9), configured.toBuilder().build().maintenance().batchPartitionGrace());
  }

  @Test
  void rejectsNonpositiveOptions() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().maintenance(m -> m.batchMaxPartitions(0)));
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().maintenance(m -> m.batchPartitionGrace(Duration.ZERO)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RatchetOptions.builder()
                .maintenance(m -> m.batchPartitionGrace(Duration.ofSeconds(-1))));
  }
}
