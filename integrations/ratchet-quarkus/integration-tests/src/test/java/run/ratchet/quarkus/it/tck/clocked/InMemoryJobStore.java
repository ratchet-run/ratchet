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
package run.ratchet.quarkus.it.tck.clocked;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.spi.RecurringJobDefinition;

/** Quarkus clocked TCK store with the recurring-store support needed during Quarkus boot. */
@ApplicationScoped
@Alternative
public class InMemoryJobStore extends run.ratchet.tck.store.clocked.InMemoryJobStore {

  private final Map<UUID, RecurringJobDefinition> recurringJobs = new HashMap<>();
  private final Map<String, UUID> recurringBusinessKeys = new HashMap<>();

  protected InMemoryJobStore() {
    super();
  }

  @Inject
  public InMemoryJobStore(Clock clock) {
    super(clock);
  }

  @Override
  public synchronized void reset() {
    super.reset();
    recurringJobs.clear();
    recurringBusinessKeys.clear();
  }

  @Override
  public synchronized List<RecurringJobDefinition> claimDueRecurring(
      int limit, String nodeId, NodeTagFilter tagFilter) {
    return Collections.emptyList();
  }

  @Override
  public synchronized Optional<Instant> findEarliestRecurringNextFire() {
    Instant earliest = null;
    for (RecurringJobDefinition definition : recurringJobs.values()) {
      if (definition.paused() || definition.nextFire() == null) {
        continue;
      }
      if (earliest == null || definition.nextFire().isBefore(earliest)) {
        earliest = definition.nextFire();
      }
    }
    return Optional.ofNullable(earliest);
  }

  @Override
  public synchronized int cancelOrphanedRecurringAnnotationJobs(
      Set<String> knownBusinessKeys, Instant nodeStartTime) {
    List<UUID> orphanIds = new ArrayList<>();
    for (RecurringJobDefinition definition : recurringJobs.values()) {
      String businessKey = definition.businessKey();
      if (businessKey != null
          && !knownBusinessKeys.contains(businessKey)
          && definition.createdAt().isBefore(nodeStartTime)) {
        orphanIds.add(definition.id());
      }
    }
    orphanIds.forEach(this::removeRecurring);
    return orphanIds.size();
  }

  @Override
  public synchronized UUID createRecurring(RecurringJobDefinition definition) {
    putRecurring(definition);
    return definition.id();
  }

  @Override
  public synchronized boolean updateRecurring(UUID id, RecurringJobDefinition definition) {
    if (!recurringJobs.containsKey(id)) {
      return false;
    }
    if (!id.equals(definition.id())) {
      throw new IllegalArgumentException("recurring definition id must match update id");
    }
    putRecurring(definition);
    return true;
  }

  @Override
  public synchronized Optional<RecurringJobDefinition> getRecurring(UUID id) {
    return Optional.ofNullable(recurringJobs.get(id));
  }

  @Override
  public synchronized Optional<RecurringJobDefinition> findRecurringByBusinessKey(
      String businessKey) {
    if (businessKey == null) {
      return Optional.empty();
    }
    UUID id = recurringBusinessKeys.get(businessKey);
    return id == null ? Optional.empty() : Optional.ofNullable(recurringJobs.get(id));
  }

  private void putRecurring(RecurringJobDefinition definition) {
    RecurringJobDefinition previous = recurringJobs.put(definition.id(), definition);
    if (previous != null && previous.businessKey() != null) {
      recurringBusinessKeys.remove(previous.businessKey());
    }
    if (definition.businessKey() != null) {
      recurringBusinessKeys.put(definition.businessKey(), definition.id());
    }
  }

  private void removeRecurring(UUID id) {
    RecurringJobDefinition removed = recurringJobs.remove(id);
    if (removed != null && removed.businessKey() != null) {
      recurringBusinessKeys.remove(removed.businessKey());
    }
  }
}
