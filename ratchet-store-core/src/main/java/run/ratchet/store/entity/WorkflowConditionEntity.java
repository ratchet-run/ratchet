package run.ratchet.store.entity;

import run.ratchet.api.WorkflowCondition;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Entity representing conditional dependencies between jobs in complex workflows.
 *
 * <p>This entity enables sophisticated workflow patterns by defining conditions that control when
 * child jobs execute based on parent job results.
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
public class WorkflowConditionEntity implements Serializable {

  @Serial private static final long serialVersionUID = -7889663048175841844L;

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "parent_job_id", nullable = false)
  private Long parentJobId;

  @Column(name = "child_job_id", nullable = false)
  private Long childJobId;

  @Enumerated(EnumType.STRING)
  @Column(name = "condition_type", nullable = false)
  private WorkflowCondition.ConditionType conditionType;

  @Lob
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

  // ── Constructors ─────────────────────────────────────────────────────────

  public WorkflowConditionEntity() {}

  // ── Getters ──────────────────────────────────────────────────────────────

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getParentJobId() {
    return parentJobId;
  }

  public void setParentJobId(Long parentJobId) {
    this.parentJobId = parentJobId;
  }

  public Long getChildJobId() {
    return childJobId;
  }

  public void setChildJobId(Long childJobId) {
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

  // ── Setters ──────────────────────────────────────────────────────────────

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

  // ── Custom methods ──────────────────────────────────────────────────────

  /**
   * Retrieves the condition expression as a deserialized object.
   *
   * <p>Returns the raw condition expression string. Actual deserialization into executable
   * predicates is handled by the workflow condition evaluator in the RI module.
   *
   * @return the condition expression string, or null if not set
   */
  public Serializable getConditionExpressionDeserialized() {
    return conditionExpression;
  }

  /**
   * Sets the condition expression from a serializable object.
   *
   * <p>Stores the string representation of the expression. Complex serialization (e.g., lambda
   * serialization) is handled by the RI module before calling this method.
   *
   * @param expression the condition expression to store
   */
  public void setConditionExpressionSerialized(Serializable expression) {
    if (expression == null) {
      this.conditionExpression = null;
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

  // ── Object overrides ────────────────────────────────────────────────────

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
