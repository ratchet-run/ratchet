package run.ratchet.api;

import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Immutable filter criteria for {@link JobQueryService} queries.
 *
 * <p>All fields are optional; null means "no constraint on this field." Construct via {@link
 * #builder()}.
 *
 * <pre>{@code
 * JobFilter filter = JobFilter.builder()
 *     .statuses(JobStatus.PENDING, JobStatus.FAILED)
 *     .types(JobType.SINGLE)
 *     .tags("billing")
 *     .createdAfter(Instant.now().minus(7, ChronoUnit.DAYS))
 *     .build();
 * }</pre>
 *
 * @param statuses allowed job statuses, or null for all statuses
 * @param types allowed job types, or null for all types
 * @param priorities allowed priorities, or null for all priorities
 * @param businessKey exact business key to match, or null for no business-key filter
 * @param idempotencyKey exact idempotency key to match, or null for no idempotency-key filter
 * @param tags tags that must be present according to the store's tag-match semantics, or null for
 *     no tag filter
 * @param targetClass exact payload target class name to match, or null for no target filter
 * @param callerPrincipal submitting principal to match, or null for no principal filter
 * @param pickedBy executor node currently owning the job, or null for no owner filter
 * @param resourceName resource permit name to match, or null for no resource filter
 * @param traceCorrelationId trace correlation identifier to match, or null for no trace filter
 * @param parentJobId parent or dependency job id to match, or null for no parent filter
 * @param createdAfter lower bound for creation time, inclusive when supported by the store
 * @param createdBefore upper bound for creation time, inclusive when supported by the store
 * @param scheduledAfter lower bound for scheduled time, inclusive when supported by the store
 * @param scheduledBefore upper bound for scheduled time, inclusive when supported by the store
 * @param updatedAfter lower bound for update time, inclusive when supported by the store
 * @param sortField field used for result ordering; defaults to {@link JobQuerySortField#CREATED_AT}
 * @param sortAscending true for ascending order, false for descending order
 * @param skipCount true to skip the total-count query and return {@code -1} as total count
 * @param includeArchived true to include archived jobs when the backing store supports archives
 * @param cursor opaque keyset-pagination cursor, or null to use offset-based pagination
 * @since 0.1
 */
@Incubating
public record JobFilter(
    Set<JobStatus> statuses,
    Set<JobType> types,
    Set<JobPriority> priorities,
    String businessKey,
    String idempotencyKey,
    Set<String> tags,
    String targetClass,
    String callerPrincipal,
    String pickedBy,
    String resourceName,
    String traceCorrelationId,
    UUID parentJobId,
    Instant createdAfter,
    Instant createdBefore,
    Instant scheduledAfter,
    Instant scheduledBefore,
    Instant updatedAfter,
    JobQuerySortField sortField,
    boolean sortAscending,
    boolean skipCount,
    boolean includeArchived,
    String cursor) {

  public JobFilter {
    statuses = copyOrNull(statuses);
    types = copyOrNull(types);
    priorities = copyOrNull(priorities);
    tags = copyOrNull(tags);
  }

  public static Builder builder() {
    return new Builder();
  }

  private static <T> Set<T> copyOrNull(Set<T> values) {
    return values == null ? null : Collections.unmodifiableSet(new HashSet<>(values));
  }

  /**
   * Returns a {@link Builder} pre-populated with every field of this filter. Safe to use in {@link
   * run.ratchet.spi.JobAuthorizationPolicy#filterForPrincipal} implementations so that injecting a
   * single field (e.g. {@code callerPrincipal}) does not silently discard the caller's original
   * filter criteria:
   *
   * <pre>{@code
   * return filter.toBuilder().callerPrincipal(principal).build();
   * }</pre>
   */
  public Builder toBuilder() {
    Builder b = new Builder();
    b.statuses = this.statuses;
    b.types = this.types;
    b.priorities = this.priorities;
    b.businessKey = this.businessKey;
    b.idempotencyKey = this.idempotencyKey;
    b.tags = this.tags;
    b.targetClass = this.targetClass;
    b.callerPrincipal = this.callerPrincipal;
    b.pickedBy = this.pickedBy;
    b.resourceName = this.resourceName;
    b.traceCorrelationId = this.traceCorrelationId;
    b.parentJobId = this.parentJobId;
    b.createdAfter = this.createdAfter;
    b.createdBefore = this.createdBefore;
    b.scheduledAfter = this.scheduledAfter;
    b.scheduledBefore = this.scheduledBefore;
    b.updatedAfter = this.updatedAfter;
    b.sortField = this.sortField;
    b.sortAscending = this.sortAscending;
    b.skipCount = this.skipCount;
    b.includeArchived = this.includeArchived;
    b.cursor = this.cursor;
    return b;
  }

  public static final class Builder {

    private Set<JobStatus> statuses;
    private Set<JobType> types;
    private Set<JobPriority> priorities;
    private String businessKey;
    private String idempotencyKey;
    private Set<String> tags;
    private String targetClass;
    private String callerPrincipal;
    private String pickedBy;
    private String resourceName;
    private String traceCorrelationId;
    private UUID parentJobId;
    private Instant createdAfter;
    private Instant createdBefore;
    private Instant scheduledAfter;
    private Instant scheduledBefore;
    private Instant updatedAfter;
    private JobQuerySortField sortField = JobQuerySortField.CREATED_AT;
    private boolean sortAscending = false;
    private boolean skipCount = false;
    private boolean includeArchived = false;
    private String cursor;

    private Builder() {}

    public Builder statuses(JobStatus... values) {
      if (values.length == 0) {
        return this;
      }
      this.statuses = Collections.unmodifiableSet(EnumSet.copyOf(Set.of(values)));
      return this;
    }

    public Builder statuses(Set<JobStatus> values) {
      this.statuses = copyOrNull(values);
      return this;
    }

    public Builder types(JobType... values) {
      if (values.length == 0) {
        return this;
      }
      this.types = Collections.unmodifiableSet(EnumSet.copyOf(Set.of(values)));
      return this;
    }

    public Builder types(Set<JobType> values) {
      this.types = copyOrNull(values);
      return this;
    }

    public Builder priorities(JobPriority... values) {
      if (values.length == 0) {
        return this;
      }
      this.priorities = Collections.unmodifiableSet(EnumSet.copyOf(Set.of(values)));
      return this;
    }

    public Builder priorities(Set<JobPriority> values) {
      this.priorities = copyOrNull(values);
      return this;
    }

    public Builder businessKey(String businessKey) {
      this.businessKey = businessKey;
      return this;
    }

    public Builder idempotencyKey(String idempotencyKey) {
      this.idempotencyKey = idempotencyKey;
      return this;
    }

    public Builder tags(String... values) {
      if (values.length == 0) {
        return this;
      }
      // Multiple tags use any-tag (OR) semantics in store queries.
      this.tags = Collections.unmodifiableSet(new HashSet<>(Set.of(values)));
      return this;
    }

    public Builder tags(Set<String> values) {
      // Multiple tags use any-tag (OR) semantics in store queries.
      this.tags = copyOrNull(values);
      return this;
    }

    public Builder targetClass(String targetClass) {
      this.targetClass = targetClass;
      return this;
    }

    public Builder callerPrincipal(String callerPrincipal) {
      this.callerPrincipal = callerPrincipal;
      return this;
    }

    public Builder pickedBy(String pickedBy) {
      this.pickedBy = pickedBy;
      return this;
    }

    public Builder resourceName(String resourceName) {
      this.resourceName = resourceName;
      return this;
    }

    public Builder traceCorrelationId(String traceCorrelationId) {
      this.traceCorrelationId = traceCorrelationId;
      return this;
    }

    public Builder parentJobId(UUID parentJobId) {
      this.parentJobId = parentJobId;
      return this;
    }

    public Builder createdAfter(Instant createdAfter) {
      this.createdAfter = createdAfter;
      return this;
    }

    public Builder createdBefore(Instant createdBefore) {
      this.createdBefore = createdBefore;
      return this;
    }

    public Builder scheduledAfter(Instant scheduledAfter) {
      this.scheduledAfter = scheduledAfter;
      return this;
    }

    public Builder scheduledBefore(Instant scheduledBefore) {
      this.scheduledBefore = scheduledBefore;
      return this;
    }

    public Builder updatedAfter(Instant updatedAfter) {
      this.updatedAfter = updatedAfter;
      return this;
    }

    public Builder sortField(JobQuerySortField sortField) {
      this.sortField = sortField;
      return this;
    }

    public Builder sortAscending(boolean sortAscending) {
      this.sortAscending = sortAscending;
      return this;
    }

    public Builder skipCount(boolean skipCount) {
      this.skipCount = skipCount;
      return this;
    }

    public Builder includeArchived(boolean includeArchived) {
      this.includeArchived = includeArchived;
      return this;
    }

    public Builder cursor(String cursor) {
      this.cursor = cursor;
      return this;
    }

    public JobFilter build() {
      return new JobFilter(
          statuses,
          types,
          priorities,
          businessKey,
          idempotencyKey,
          tags,
          targetClass,
          callerPrincipal,
          pickedBy,
          resourceName,
          traceCorrelationId,
          parentJobId,
          createdAfter,
          createdBefore,
          scheduledAfter,
          scheduledBefore,
          updatedAfter,
          sortField,
          sortAscending,
          skipCount,
          includeArchived,
          cursor);
    }
  }
}
