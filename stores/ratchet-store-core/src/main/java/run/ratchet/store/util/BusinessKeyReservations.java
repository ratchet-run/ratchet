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
package run.ratchet.store.util;

import java.util.List;
import run.ratchet.store.entity.JobExecutionType;

/** Shared owner-table values for active business-key reservations. */
public final class BusinessKeyReservations {

  public static final String OWNER_TABLE_QUEUE = "QUEUE";
  public static final String OWNER_TABLE_RECURRING = "RECURRING";

  private BusinessKeyReservations() {}

  public static String ownerTableFor(JobExecutionType jobType) {
    return jobType == JobExecutionType.RECURRING ? OWNER_TABLE_RECURRING : OWNER_TABLE_QUEUE;
  }

  public static String ownerTableFor(String jobType) {
    return "RECURRING".equals(jobType) ? OWNER_TABLE_RECURRING : OWNER_TABLE_QUEUE;
  }

  /**
   * Returns the order in which every hold sweep deletes the keys it selected. The expiry sweep and
   * the node release each delete many rows in one transaction, so a shared order keeps them from
   * locking the same rows in opposite order.
   */
  public static List<String> holdDeleteOrder(List<?> keys) {
    return keys.stream().map(String.class::cast).sorted().toList();
  }
}
