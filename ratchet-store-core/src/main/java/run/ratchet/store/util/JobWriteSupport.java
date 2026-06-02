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

import java.util.Objects;
import java.util.UUID;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;

/** Shared save-path helpers used by SQL store write operations. */
public final class JobWriteSupport {

  private static final String DEFAULT_CRON_EXPR = "";
  private static final String DEFAULT_ZONE_ID = "UTC";

  private JobWriteSupport() {}

  /**
   * Coerces an optional cron expression to the stored default. A regular (non-recurring) job
   * legitimately has no cron expression, so a null value is normalized to an empty string rather
   * than rejected, keeping the cold-insert path identical across SQL stores.
   *
   * @param cronExpr the cron expression, possibly {@code null}.
   * @return the cron expression, or {@code ""} when {@code null}.
   */
  public static String coerceCronExpr(String cronExpr) {
    return cronExpr == null ? DEFAULT_CRON_EXPR : cronExpr;
  }

  /**
   * Coerces an optional zone id to the stored default. A null zone sensibly defaults to {@code
   * UTC}, matching the recurring-job insert path and the other stores, rather than being rejected.
   *
   * @param zoneId the zone id, possibly {@code null}.
   * @return the zone id, or {@code "UTC"} when {@code null}.
   */
  public static String coerceZoneId(String zoneId) {
    return zoneId == null ? DEFAULT_ZONE_ID : zoneId;
  }

  public static void checkHotField(UUID jobId, String fieldName, Object incoming, Object stored) {
    if (Objects.equals(incoming, stored)) {
      return;
    }
    throw new IllegalStateException(
        "save() rejected: hot-field mutation detected for id="
            + jobId
            + " field="
            + fieldName
            + " incoming="
            + incoming
            + " stored="
            + stored
            + ". Use an explicit transition method.");
  }

  public static void assignIdIfMissing(JobEntity job) {
    if (job.getId() == null) {
      job.setId(UuidV7Factory.create());
    }
  }
}
