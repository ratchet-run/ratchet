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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.transaction.Transactional;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.dto.PartitionGenerationWrite;
import run.ratchet.store.entity.JobEntity;

/**
 * Contract for JPA-backed {@code JobStore} implementations that express their lock and
 * node-liveness transaction boundaries through Jakarta Transactions.
 *
 * <p>Lock acquisition and release, lease renewal, heartbeat upserts, and inactive-node cleanup must
 * commit in a transaction independent of any caller transaction. That way a distributed lock or a
 * liveness write becomes visible to other nodes as soon as the operation returns and is not undone
 * if the caller later rolls back. JPA stores satisfy this with {@code @Transactional(REQUIRES_NEW)}
 * on those methods, and this contract pins the attribute so the SQL stores cannot silently drift
 * apart again (one store once carried the annotation while its sibling did not).
 *
 * <p>Stores that achieve independent commit through single-operation atomicity rather than Jakarta
 * Transactions (for example document stores) are exempt and do not extend this contract.
 */
public abstract class AbstractJobStoreTransactionBoundaryContract {

  /** The concrete {@code JobStore} implementation class whose annotations are under test. */
  protected abstract Class<?> jobStoreImplClass();

  @Test
  void createExemptsOnlyWriteFreeReservationConflicts() throws NoSuchMethodException {
    Transactional classLevel = jobStoreImplClass().getAnnotation(Transactional.class);
    Transactional create =
        jobStoreImplClass().getMethod("create", JobEntity.class).getAnnotation(Transactional.class);
    assertNotNull(create, "create must preserve the caller's transaction on a held reservation");
    assertEquals(classLevel.value(), create.value());
    assertArrayEquals(classLevel.rollbackOn(), create.rollbackOn());
    assertArrayEquals(
        new Class<?>[] {
          BusinessKeyReservationConflictException.class, DuplicateIdempotencyKeyException.class
        },
        create.dontRollbackOn());
    Transactional save =
        jobStoreImplClass().getMethod("save", JobEntity.class).getAnnotation(Transactional.class);
    assertFalse(
        exemptsReservationConflict(classLevel.dontRollbackOn()),
        "the class-level default must roll back on a reservation conflict");
    assertFalse(
        exemptsReservationConflict(
            save == null ? classLevel.dontRollbackOn() : save.dontRollbackOn()),
        "save must retain its normal rollback behavior");
  }

  @Test
  void createOrAdvanceExemptsOnlyHeldParentKey() throws NoSuchMethodException {
    Transactional boundary =
        jobStoreImplClass()
            .getMethod("createOrAdvance", PartitionGenerationWrite.class)
            .getAnnotation(Transactional.class);
    assertNotNull(boundary);
    assertEquals(Transactional.TxType.REQUIRED, boundary.value());
    assertArrayEquals(
        new Class<?>[] {DuplicateIdempotencyKeyException.class}, boundary.dontRollbackOn());
    assertArrayEquals(new Class<?>[0], boundary.rollbackOn());
  }

  private static boolean exemptsReservationConflict(Class<?>[] dontRollbackOn) {
    return Arrays.stream(dontRollbackOn)
        .anyMatch(
            type ->
                type.isAssignableFrom(BusinessKeyReservationConflictException.class)
                    || type.isAssignableFrom(DuplicateIdempotencyKeyException.class));
  }

  @Test
  void tryLockCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("tryLock", String.class, Duration.class, String.class);
  }

  @Test
  void unlockCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("unlock", String.class, String.class);
  }

  @Test
  void renewLockCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("renewLock", String.class, Duration.class, String.class);
  }

  @Test
  void upsertHeartbeatCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("upsertHeartbeat", String.class, Instant.class);
  }

  @Test
  void deleteInactiveNodesSinceCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("deleteInactiveNodesSince", Instant.class);
  }

  @Test
  void deleteInactiveNodesByIdsCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("deleteInactiveNodesByIds", Collection.class);
  }

  @Test
  void currentClaimReadsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("currentClaim", UUID.class);
  }

  @Test
  void classLevelBoundaryIsRequired() {
    Transactional classLevel = jobStoreImplClass().getAnnotation(Transactional.class);
    assertNotNull(classLevel, "the store impl must carry a class-level @Transactional default");
    assertEquals(
        Transactional.TxType.REQUIRED,
        classLevel.value(),
        "every store method without its own attribute must default to a REQUIRED tx boundary");
  }

  private void assertRequiresNew(String method, Class<?>... parameterTypes)
      throws NoSuchMethodException {
    Transactional tx =
        jobStoreImplClass().getMethod(method, parameterTypes).getAnnotation(Transactional.class);
    assertNotNull(tx, method + " must declare @Transactional so it gets its own transaction");
    assertEquals(
        Transactional.TxType.REQUIRES_NEW,
        tx.value(),
        method + " must commit in a transaction independent of the caller (REQUIRES_NEW)");
  }
}
