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
package run.ratchet.testsuite.event;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import run.ratchet.api.event.JobsBulkCancelledEvent;

/**
 * CDI observer for {@link JobsBulkCancelledEvent}. The standalone (non-{@code
 * AbstractJobSchedulerEvent}) shape of the bulk event prevents the generic {@code EventCapture}
 * from picking it up — bulk events need a dedicated observer.
 */
@ApplicationScoped
public class BulkCancelEventCapture {

  private final CopyOnWriteArrayList<JobsBulkCancelledEvent> events = new CopyOnWriteArrayList<>();

  public void onEvent(@Observes JobsBulkCancelledEvent event) {
    events.add(event);
  }

  public List<JobsBulkCancelledEvent> getEvents() {
    return List.copyOf(events);
  }

  public void clear() {
    events.clear();
  }
}
