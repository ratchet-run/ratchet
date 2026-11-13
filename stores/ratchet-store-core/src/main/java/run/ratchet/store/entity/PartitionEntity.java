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

import java.util.UUID;

/**
 * Durable key range and latest generation outcome for one partition.
 *
 * <p>A null outcome means the generation is not sealed. Checkpoints are opaque large text and may
 * contain ciphertext; stores must preserve them without interpreting or truncating their contents.
 */
public class PartitionEntity {
  private UUID setId;
  private int partitionIndex;
  private long rangeFrom;
  private long rangeTo;
  private int generation;
  private UUID childJobId;
  private PartitionOutcome outcome;
  private String lastError;
  private String checkpoint;

  public UUID getSetId() {
    return setId;
  }

  public void setSetId(UUID setId) {
    this.setId = setId;
  }

  public int getPartitionIndex() {
    return partitionIndex;
  }

  public void setPartitionIndex(int partitionIndex) {
    this.partitionIndex = partitionIndex;
  }

  public long getRangeFrom() {
    return rangeFrom;
  }

  public void setRangeFrom(long rangeFrom) {
    this.rangeFrom = rangeFrom;
  }

  public long getRangeTo() {
    return rangeTo;
  }

  public void setRangeTo(long rangeTo) {
    this.rangeTo = rangeTo;
  }

  public int getGeneration() {
    return generation;
  }

  public void setGeneration(int generation) {
    this.generation = generation;
  }

  public UUID getChildJobId() {
    return childJobId;
  }

  public void setChildJobId(UUID childJobId) {
    this.childJobId = childJobId;
  }

  public PartitionOutcome getOutcome() {
    return outcome;
  }

  public void setOutcome(PartitionOutcome outcome) {
    this.outcome = outcome;
  }

  public String getLastError() {
    return lastError;
  }

  public void setLastError(String lastError) {
    this.lastError = lastError;
  }

  public String getCheckpoint() {
    return checkpoint;
  }

  public void setCheckpoint(String checkpoint) {
    this.checkpoint = checkpoint;
  }
}
