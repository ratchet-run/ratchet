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
package example.ratchet;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "consumer_uuid_record")
public class ConsumerUuidRecord {
  @Id private UUID id;

  @Column(length = 80, nullable = false)
  private ConsumerLabel label;

  @Column(name = "occurred_at", nullable = false)
  private Instant occurredAt;

  public ConsumerUuidRecord() {}

  public ConsumerUuidRecord(UUID id, ConsumerLabel label, Instant occurredAt) {
    this.id = id;
    this.label = label;
    this.occurredAt = occurredAt;
  }

  public UUID getId() {
    return id;
  }

  public ConsumerLabel getLabel() {
    return label;
  }

  public Instant getOccurredAt() {
    return occurredAt;
  }
}
