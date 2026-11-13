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
import jakarta.transaction.Transactional;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobCrudStore;

/** Reads cancellation state outside the caller's transaction. */
@ApplicationScoped
public class CancelStatusReader {
  protected CancelStatusReader() {}

  @Inject
  public CancelStatusReader(JobCrudStore jobCrudStore) {}

  @Transactional(Transactional.TxType.NOT_SUPPORTED)
  public Map<UUID, JobStatus> findStatuses(Collection<UUID> ids) {
    throw new UnsupportedOperationException("not implemented yet");
  }
}
