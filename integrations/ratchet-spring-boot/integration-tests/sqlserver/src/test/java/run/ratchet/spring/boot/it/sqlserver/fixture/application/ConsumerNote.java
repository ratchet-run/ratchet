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
package run.ratchet.spring.boot.it.sqlserver.fixture.application;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "consumer_note")
public class ConsumerNote {

  @Id private UUID id;

  @Column(nullable = false)
  private String message;

  protected ConsumerNote() {}

  public ConsumerNote(UUID id, String message) {
    this.id = id;
    this.message = message;
  }

  public UUID getId() {
    return id;
  }

  public String getMessage() {
    return message;
  }
}
