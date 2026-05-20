package run.ratchet.store.spi;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.NodeTagFilter;

/**
 * Dedicated SPI for recurring-master persistence. Recurring masters live in their own table /
 * collection ({@code scheduler_recurring_job}) post CP2 split, and have no executable-queue
 * lifecycle. Every method on this sub-interface has a concrete consumer in the RI executor,
 * scheduler, registration path, admin path, or query layer.
 *
 * <p>Cancellation never updates the live master in-place; it deletes the live row and inserts a
 * denormalized snapshot into {@code scheduler_recurring_job_archive} atomically.
 *
 * <p>Compose into {@link JobStore}.
 */
@Incubating
public interface RecurringJobStore {

  /**
   * Reason a recurring definition was moved to the archive.
   *
   * <ul>
   *   <li>{@link #CANCELED}: explicit user / admin cancel, or annotation orphan cleanup at node
   *       startup.
   *   <li>{@link #EXHAUSTED}: cron expression yielded no next fire after the catch-up loop.
   * </ul>
   */
  enum ArchiveReason {
    CANCELED,
    EXHAUSTED
  }

  /**
   * Claims up to {@code limit} due recurring masters (unpaused, {@code next_fire <= now}) with row
   * locking semantics equivalent to {@code FOR UPDATE SKIP LOCKED}. Mongo uses single-document
   * atomicity via {@code findOneAndUpdate}. Transaction attribute: {@code REQUIRED}.
   */
  List<RecurringJobDefinition> claimDueRecurring(int limit, String nodeId, NodeTagFilter tagFilter);

  /** {@link NodeTagFilter#NONE} overload. */
  default List<RecurringJobDefinition> claimDueRecurring(int limit, String nodeId) {
    return claimDueRecurring(limit, nodeId, NodeTagFilter.NONE);
  }

  /**
   * Advances the master's {@code next_fire} atomically within the claim transaction. Called once
   * per fire after the child job is enqueued. Transaction attribute: {@code REQUIRED}.
   */
  void advanceNextFire(UUID id, Instant nextFire);

  /**
   * Returns the earliest pending {@code next_fire} across all unpaused recurring masters. Used by
   * the scheduler's adaptive sleep. Transaction attribute: {@code SUPPORTS}.
   */
  Optional<Instant> findEarliestRecurringNextFire();

  /**
   * CAS pause: flips {@code is_paused=true, paused_at=now} only when the current row is active.
   * Returns {@code true} iff the row transitioned. Idempotent: a second concurrent call returns
   * {@code false}. Transaction attribute: {@code REQUIRED}.
   */
  boolean pauseRecurring(UUID id);

  /**
   * CAS resume: flips {@code is_paused=false, paused_at=null} only when the current row is paused.
   * Returns {@code true} iff the row transitioned. Idempotent. Transaction attribute: {@code
   * REQUIRED}.
   */
  boolean resumeRecurring(UUID id);

  /**
   * Hard-deletes the live row and inserts the denormalized snapshot into {@code
   * scheduler_recurring_job_archive} in a single transaction. Also deletes the associated bkres row
   * when present. Returns {@code true} iff the live row was deleted. Idempotent: returns {@code
   * false} when the id is unknown. Transaction attribute: {@code REQUIRED}.
   */
  boolean cancelRecurringAndArchive(UUID id, ArchiveReason reason);

  /**
   * Node-startup cleanup. Cancels (with reason {@link ArchiveReason#CANCELED}) every recurring
   * master whose {@code business_key} no longer appears in the supplied {@code knownBusinessKeys}
   * set AND was created before {@code nodeStartTime}. Used to retire {@code @Recurring} annotation
   * jobs whose backing method has been removed or renamed. Returns the number of masters canceled.
   * Transaction attribute: {@code REQUIRED}.
   */
  int cancelOrphanedRecurringAnnotationJobs(Set<String> knownBusinessKeys, Instant nodeStartTime);

  /**
   * Bulk cancel-by-tag. Cancels every recurring master joined to {@code tag} via {@code
   * scheduler_job_tag}; each row is archived with reason {@link ArchiveReason#CANCELED}. Returns
   * the number of masters canceled. Transaction attribute: {@code REQUIRED}.
   */
  int cancelRecurringJobsByTag(String tag);

  /**
   * Cancels the single recurring master matching {@code businessKey}, if any. Returns {@code true}
   * when a row was canceled. Transaction attribute: {@code REQUIRED}.
   *
   * <p>Named {@code cancelRecurringJobByBusinessKey} (not {@code cancelRecurringJobByBusinessKey})
   * to avoid signature collision with the pre-CP2 {@code int} return on {@code
   * JobBatchStatusStore}. After the legacy method is removed in the final CP2 commit, this can be
   * renamed if a consumer expects the historical name.
   */
  boolean cancelRecurringJobByBusinessKey(String businessKey);

  /**
   * Bulk cancel by business key set. Returns the number of masters canceled. Transaction attribute:
   * {@code REQUIRED}.
   */
  int cancelRecurringJobsByBusinessKeys(Set<String> businessKeys);

  /**
   * Inserts a new recurring definition row and returns its id. Transaction attribute: {@code
   * REQUIRED}.
   */
  UUID createRecurring(RecurringJobDefinition definition);

  /**
   * Updates an existing recurring definition (cron, zone, template fields, etc.). The id of {@code
   * definition} identifies the target row. Returns {@code true} iff a row was updated. Transaction
   * attribute: {@code REQUIRED}.
   */
  boolean updateRecurring(UUID id, RecurringJobDefinition definition);

  /** Reads a single recurring definition by id. Transaction attribute: {@code SUPPORTS}. */
  Optional<RecurringJobDefinition> getRecurring(UUID id);

  /** Registration-path lookup by business key. Transaction attribute: {@code SUPPORTS}. */
  Optional<RecurringJobDefinition> findRecurringByBusinessKey(String businessKey);

  /**
   * Lists every live recurring master (used by {@code JobQueryService.getRecurringMasters()} and by
   * recurring-flow integration tests). Transaction attribute: {@code SUPPORTS}.
   */
  List<RecurringJobDefinition> listAll();
}
