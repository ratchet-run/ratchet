package run.ratchet.store.entity;

import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.id.UuidV7EntityListener;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

/**
 * Conditional dependency between jobs in a workflow.
 *
 * @see WorkflowCondition
 */
@Entity
@Table(
    name = "scheduler_workflow_condition",
    indexes = {
      @Index(name = "idx_workflow_parent", columnList = "parent_job_id"),
      @Index(name = "idx_workflow_child", columnList = "child_job_id"),
      @Index(name = "idx_workflow_priority", columnList = "parent_job_id, condition_priority")
    })
@EntityListeners(UuidV7EntityListener.class)
public class WorkflowConditionEntity
    implements Serializable, UuidV7EntityListener.UuidV7Assignable {

  @Serial private static final long serialVersionUID = -7889663048175841844L;

  @Id private UUID id;

  @Column(name = "parent_job_id", nullable = false)
  private UUID parentJobId;

  @Column(name = "child_job_id", nullable = false)
  private UUID childJobId;

  @Enumerated(EnumType.STRING)
  @Column(name = "condition_type", nullable = false)
  private WorkflowCondition.ConditionType conditionType;

  @Column(name = "condition_expression")
  private String conditionExpression;

  @Column(name = "condition_priority", nullable = false)
  private Integer conditionPriority = 0;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "parent_job_id", insertable = false, updatable = false)
  private transient JobEntity parentJob;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "child_job_id", insertable = false, updatable = false)
  private transient JobEntity childJob;

  public WorkflowConditionEntity() {}

  private static String serializeExpression(Serializable expression) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(expression);
      oos.flush();
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (IOException e) {
      throw new IllegalArgumentException("Condition expression serialization error", e);
    }
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public UUID getParentJobId() {
    return parentJobId;
  }

  public void setParentJobId(UUID parentJobId) {
    this.parentJobId = parentJobId;
  }

  public UUID getChildJobId() {
    return childJobId;
  }

  public void setChildJobId(UUID childJobId) {
    this.childJobId = childJobId;
  }

  public WorkflowCondition.ConditionType getConditionType() {
    return conditionType;
  }

  public void setConditionType(WorkflowCondition.ConditionType conditionType) {
    this.conditionType = conditionType;
  }

  public String getConditionExpression() {
    return conditionExpression;
  }

  public void setConditionExpression(String conditionExpression) {
    this.conditionExpression = conditionExpression;
  }

  public Integer getConditionPriority() {
    return conditionPriority;
  }

  public void setConditionPriority(Integer conditionPriority) {
    this.conditionPriority = conditionPriority;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Instant createdAt) {
    this.createdAt = createdAt;
  }

  public JobEntity getParentJob() {
    return parentJob;
  }

  public void setParentJob(JobEntity parentJob) {
    this.parentJob = parentJob;
  }

  public JobEntity getChildJob() {
    return childJob;
  }

  public void setChildJob(JobEntity childJob) {
    this.childJob = childJob;
  }

  /** Returns the raw condition expression string; deserialization is handled by the RI module. */
  public Serializable getConditionExpressionDeserialized() {
    return conditionExpression;
  }

  /** Stores the serialized form of the expression; lambda serialization is handled by the RI. */
  public void setConditionExpressionSerialized(Serializable expression) {
    if (expression == null) {
      this.conditionExpression = null;
    } else if (expression instanceof SerializablePredicate<?>
        || expression instanceof SerializableFunction<?, ?>) {
      this.conditionExpression = serializeExpression(expression);
    } else {
      this.conditionExpression = expression.toString();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkflowConditionEntity that = (WorkflowConditionEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(parentJobId, that.parentJobId)
        && Objects.equals(childJobId, that.childJobId)
        && conditionType == that.conditionType
        && Objects.equals(conditionExpression, that.conditionExpression)
        && Objects.equals(conditionPriority, that.conditionPriority)
        && Objects.equals(createdAt, that.createdAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        parentJobId,
        childJobId,
        conditionType,
        conditionExpression,
        conditionPriority,
        createdAt);
  }

  @Override
  public String toString() {
    return "WorkflowConditionEntity("
        + "id="
        + id
        + ", parentJobId="
        + parentJobId
        + ", childJobId="
        + childJobId
        + ", conditionType="
        + conditionType
        + ", conditionExpression="
        + conditionExpression
        + ", conditionPriority="
        + conditionPriority
        + ", createdAt="
        + createdAt
        + ')';
  }

  @PrePersist
  void prePersist() {
    if (createdAt == null) {
      createdAt = Instant.now();
    }
  }
}
