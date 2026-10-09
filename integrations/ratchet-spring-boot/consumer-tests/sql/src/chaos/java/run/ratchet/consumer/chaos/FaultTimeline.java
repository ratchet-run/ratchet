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
package run.ratchet.consumer.chaos;

import java.util.ArrayList;
import java.util.List;

/** Fault windows measured using the shared database clock. */
public final class FaultTimeline {
  public record Entry(
      String nodeId,
      String incarnation,
      String fault,
      long beforeUs,
      long confirmedUs,
      Long endedUs) {}

  private final List<Entry> entries = new ArrayList<>();

  public List<Entry> entries() {
    return List.copyOf(entries);
  }

  void add(Entry entry) {
    entries.add(entry);
  }

  void end(String incarnation, long time) {
    for (int i = 0; i < entries.size(); i++) {
      Entry e = entries.get(i);
      if (e.incarnation().equals(incarnation) && e.endedUs() == null)
        entries.set(
            i,
            new Entry(e.nodeId(), e.incarnation(), e.fault(), e.beforeUs(), e.confirmedUs(), time));
    }
  }
}
