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
package run.ratchet.store.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Objects;

/**
 * Configured resource concurrency limit.
 *
 * @see ResourcePermitEntity
 */
@Entity
@Table(name = "scheduler_resource_limit")
public class ResourceLimitEntity {

  public static final int DEFAULT_RETRY_DELAY_MS = 5000;

  @Id
  @Column(name = "resource_name", nullable = false, length = 100)
  private String resourceName;

  @Column(name = "max_concurrent", nullable = false)
  private int maxConcurrent;

  @Column(name = "retry_delay_ms", nullable = false)
  private int retryDelayMs = DEFAULT_RETRY_DELAY_MS;

  @Column(name = "description")
  private String description;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public int getMaxConcurrent() {
    return maxConcurrent;
  }

  public void setMaxConcurrent(int maxConcurrent) {
    this.maxConcurrent = maxConcurrent;
  }

  public int getRetryDelayMs() {
    return retryDelayMs;
  }

  public void setRetryDelayMs(int retryDelayMs) {
    this.retryDelayMs = retryDelayMs;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Instant createdAt) {
    this.createdAt = createdAt;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Instant updatedAt) {
    this.updatedAt = updatedAt;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(resourceName);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResourceLimitEntity that = (ResourceLimitEntity) o;
    return Objects.equals(resourceName, that.resourceName);
  }
}
