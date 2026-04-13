package run.ratchet.store.entity;

import run.ratchet.store.converter.JobPayloadConverter;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.util.Objects;

/**
 * Batch job progress tracking.
 *
 * @see JobEntity for the parent batch job
 */
@Entity
@Table(name = "scheduler_batch")
public class BatchEntity {

  @Id
  @Column(name = "batch_id")
  private Long id;

  @Column(name = "total_items", nullable = false)
  private int totalItems;

  @Column(name = "completed_items", nullable = false)
  private int completedItems;

  @Column(name = "failed_items", nullable = false)
  private int failedItems;

  @Column(name = "completion_processed")
  private Boolean completionProcessed = false;

  @Version
  @Column(name = "version")
  private Integer version;

  @Convert(converter = JobPayloadConverter.class)
  @Column(name = "progress_hook")
  private JobPayload progressHook;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public int getTotalItems() {
    return totalItems;
  }

  public void setTotalItems(int totalItems) {
    this.totalItems = totalItems;
  }

  public int getCompletedItems() {
    return completedItems;
  }

  public void setCompletedItems(int completedItems) {
    this.completedItems = completedItems;
  }

  public int getFailedItems() {
    return failedItems;
  }

  public void setFailedItems(int failedItems) {
    this.failedItems = failedItems;
  }

  public Boolean getCompletionProcessed() {
    return completionProcessed;
  }

  public void setCompletionProcessed(Boolean completionProcessed) {
    this.completionProcessed = completionProcessed;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public JobPayload getProgressHook() {
    return progressHook;
  }

  public void setProgressHook(JobPayload progressHook) {
    this.progressHook = progressHook;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BatchEntity that = (BatchEntity) o;
    return Objects.equals(id, that.id);
  }
}
