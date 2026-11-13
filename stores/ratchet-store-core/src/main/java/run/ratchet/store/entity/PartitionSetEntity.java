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

import java.time.Instant;
import java.util.UUID;

/** Durable logical partition set, identified by a caller key of at most 36 characters. */
public class PartitionSetEntity {
  private UUID setId;
  private String setKey;
  private String name;
  private int partitionCount;
  private int latestGeneration;
  private UUID latestBatchId;
  private int sealedGeneration;
  private Instant generationStartedAt;
  private String ownerPrincipal;
  private int version;
  private Instant createdAt;
  private Instant updatedAt;

  public UUID getSetId() {
    return setId;
  }

  public void setSetId(UUID setId) {
    this.setId = setId;
  }

  public String getSetKey() {
    return setKey;
  }

  public void setSetKey(String setKey) {
    this.setKey = setKey;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  public int getLatestGeneration() {
    return latestGeneration;
  }

  public void setLatestGeneration(int latestGeneration) {
    this.latestGeneration = latestGeneration;
  }

  public UUID getLatestBatchId() {
    return latestBatchId;
  }

  public void setLatestBatchId(UUID latestBatchId) {
    this.latestBatchId = latestBatchId;
  }

  public int getSealedGeneration() {
    return sealedGeneration;
  }

  public void setSealedGeneration(int sealedGeneration) {
    this.sealedGeneration = sealedGeneration;
  }

  public Instant getGenerationStartedAt() {
    return generationStartedAt;
  }

  public void setGenerationStartedAt(Instant generationStartedAt) {
    this.generationStartedAt = generationStartedAt;
  }

  public String getOwnerPrincipal() {
    return ownerPrincipal;
  }

  public void setOwnerPrincipal(String ownerPrincipal) {
    this.ownerPrincipal = ownerPrincipal;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
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
}
