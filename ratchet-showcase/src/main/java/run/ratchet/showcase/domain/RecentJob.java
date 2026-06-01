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
package run.ratchet.showcase.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class RecentJob {

  public String id;
  public String status;
  public String type;
  public String priority;
  public String target;
  public String method;
  public String businessKey;
  public String resource;
  public String pickedBy;
  public String error;
  public int attempts;
  public int maxRetries;
  public List<String> tags;
  public Instant createdAt;
  public Instant scheduledTime;
  public Instant updatedAt;
  public UUID dependsOn;
}
