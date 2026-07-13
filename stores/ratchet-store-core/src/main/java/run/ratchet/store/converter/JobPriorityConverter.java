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
package run.ratchet.store.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import run.ratchet.api.JobPriority;

/** Persists {@link JobPriority} through its stable integer code. */
@Converter
public class JobPriorityConverter implements AttributeConverter<JobPriority, Integer> {

  @Override
  public Integer convertToDatabaseColumn(JobPriority attribute) {
    return attribute == null ? null : attribute.persistedCode();
  }

  @Override
  public JobPriority convertToEntityAttribute(Integer dbData) {
    return dbData == null ? null : JobPriority.fromPersistedCode(dbData);
  }
}
