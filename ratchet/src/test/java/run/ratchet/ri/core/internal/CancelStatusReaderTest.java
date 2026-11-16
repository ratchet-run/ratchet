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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobCrudStore;

class CancelStatusReaderTest {
  @Test
  void findStatuses_delegatesToTheStore() {
    // Status reads forward the supplied ids and return the store's result.
    JobCrudStore store = mock(JobCrudStore.class);
    List<UUID> ids = List.of(UUID.randomUUID());
    Map<UUID, JobStatus> statuses = Map.of(ids.get(0), JobStatus.CANCELED);
    when(store.findStatusesByIds(ids)).thenReturn(statuses);
    assertSame(statuses, new CancelStatusReader(store).findStatuses(ids));
    verify(store).findStatusesByIds(ids);
  }

  @Test
  void findStatuses_runsOutsideAnyTransaction() throws Exception {
    // CDI status reads suspend any caller transaction.
    assertNotNull(CancelStatusReader.class.getAnnotation(ApplicationScoped.class));
    Transactional transactional =
        CancelStatusReader.class
            .getMethod("findStatuses", Collection.class)
            .getAnnotation(Transactional.class);
    assertNotNull(transactional);
    assertEquals(Transactional.TxType.NOT_SUPPORTED, transactional.value());
  }
}
