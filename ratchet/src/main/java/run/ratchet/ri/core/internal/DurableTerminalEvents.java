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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.event.AbstractJobSchedulerEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.OutboxEvent;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.EventOutboxStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;

/** Shared durable completion planning and local delivery lifecycle. */
@ApplicationScoped
public class DurableTerminalEvents {
  public record PreparedDlqEvents(
      List<AbstractJobSchedulerEvent> eventsBeforeDlq, JobDlqEvent dlq) {
    public PreparedDlqEvents {
      eventsBeforeDlq = List.copyOf(eventsBeforeDlq);
      Objects.requireNonNull(dlq, "dlq");
    }

    public List<AbstractJobSchedulerEvent> events() {
      List<AbstractJobSchedulerEvent> events = new ArrayList<>(eventsBeforeDlq);
      events.add(dlq);
      return List.copyOf(events);
    }
  }

  private final RatchetOptions options;
  private final JobStore store;
  private final Clock clock;
  private final InternalEventPublisher publisher;
  private final AfterCommitRegistrar registrar;
  private final SingletonLeaseService leases;
  private OutboxDeleter deleter;
  private TerminalEventRelay relay;

  protected DurableTerminalEvents() {
    options = null;
    store = null;
    clock = null;
    publisher = null;
    registrar = null;
    leases = null;
  }

  public DurableTerminalEvents(
      RatchetOptions options,
      JobStore store,
      Clock clock,
      InternalEventPublisher publisher,
      AfterCommitRegistrar registrar,
      SingletonLeaseService leases) {
    this(options, store, clock, publisher, registrar, leases, null, null);
  }

  @Inject
  public DurableTerminalEvents(
      RatchetOptions options,
      JobStore store,
      Clock clock,
      InternalEventPublisher publisher,
      AfterCommitRegistrar registrar,
      SingletonLeaseService leases,
      OutboxDeleter deleter,
      TerminalEventRelay relay) {
    this.options = options;
    this.store = store;
    this.clock = clock;
    this.publisher = publisher;
    this.registrar = registrar;
    this.leases = leases;
    this.deleter = deleter;
    this.relay = relay;
  }

  public boolean enabled() {
    return options.events().durable();
  }

  /** Validates capabilities without opening transactions or scheduling work. */
  public void validate() {
    if (!enabled()) return;
    if (store.capability(EventOutboxStore.class).isEmpty())
      throw new IllegalStateException("Durable events require EventOutboxStore");
    if (store.capability(LockStore.class).isEmpty())
      throw new IllegalStateException("Durable events require LockStore");
    if ((deleter != null && !deleter.hasStore()) || (relay != null && !relay.hasStore()))
      throw missingBeanType(EventOutboxStore.class);
    if (leases == null || !leases.hasLockStore()) throw missingBeanType(LockStore.class);
  }

  private static IllegalStateException missingBeanType(Class<?> capability) {
    return new IllegalStateException(
        "Store implements "
            + capability.getSimpleName()
            + " but does not expose it as a bean type; list it on the store bean."
            + " Per-store composites must list every capability.");
  }

  public void start(ScheduledExecutorService executor) {
    if (!enabled()) return;
    EventOutboxStore outbox = store.capability(EventOutboxStore.class).orElseThrow();
    if (deleter == null) deleter = new OutboxDeleter(outbox);
    deleter.start(executor);
    if (relay == null)
      relay =
          new TerminalEventRelay(
              outbox,
              store.capability(BatchStore.class).orElse(null),
              leases,
              publisher,
              options,
              clock);
    relay.start(executor);
  }

  public void stop() {
    if (relay != null) relay.stop();
    if (deleter != null) deleter.stop();
  }

  public TerminalEventRelay relay() {
    return relay;
  }

  public PreparedDlqEvents prepareDlqEvents(
      List<AbstractJobSchedulerEvent> before, JobDlqEvent dlq) {
    return new PreparedDlqEvents(
        prepare(before), enabled() ? (JobDlqEvent) TerminalEventMapper.withDurableId(dlq) : dlq);
  }

  public List<AbstractJobSchedulerEvent> prepare(List<AbstractJobSchedulerEvent> events) {
    if (!enabled()) return events;
    return events.stream()
        .map(event -> isTerminal(event) ? TerminalEventMapper.withDurableId(event) : event)
        .toList();
  }

  private static boolean isTerminal(AbstractJobSchedulerEvent event) {
    return event instanceof JobCompletedEvent
        || event instanceof JobFailedEvent
        || event instanceof JobDlqEvent;
  }

  public JobCompletionPlan withEvents(
      JobCompletionPlan plan, List<AbstractJobSchedulerEvent> events) {
    if (!enabled()) return plan;
    List<OutboxEvent> rows =
        events.stream()
            .filter(DurableTerminalEvents::isTerminal)
            .map(event -> TerminalEventMapper.toRow(event, clock.instant()))
            .toList();
    return new JobCompletionPlan(
        plan.jobId(),
        plan.expectedStatus(),
        plan.terminalStatus(),
        plan.resultJson(),
        plan.resultType(),
        plan.errorMessage(),
        plan.attempts(),
        plan.start(),
        plan.end(),
        plan.durationMs(),
        plan.queueWaitMs(),
        plan.batchId(),
        plan.completedBatch(),
        plan.dependencies(),
        plan.expectedClaimSeq(),
        rows);
  }

  public void publishAfterCommit(List<AbstractJobSchedulerEvent> events) {
    AfterCommitActions.runOrWarn(registrar, "durable terminal events", () -> publishNow(events));
  }

  public void publishNow(List<AbstractJobSchedulerEvent> events) {
    if (publisher == null) return;
    events.forEach(publisher::publish);
    if (enabled() && deleter != null)
      deleter.enqueue(
          events.stream()
              .filter(DurableTerminalEvents::isTerminal)
              .map(AbstractJobSchedulerEvent::getEventId)
              .toList());
  }
}
