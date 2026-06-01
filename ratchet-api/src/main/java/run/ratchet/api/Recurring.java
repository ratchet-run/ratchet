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
package run.ratchet.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a CDI bean method for automatic recurring execution on a cron schedule. Methods must be
 * public and accept either no parameters or a single {@link JobContext} parameter.
 *
 * <pre>{@code
 * &#64;ApplicationScoped
 * public class MaintenanceService {
 *
 *     &#64;Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
 *     public void performCleanup() { ... }
 *
 *     &#64;Recurring(
 *         cron = "0 0 * * * ?",
 *         priority = 8,
 *         maxRetries = 5,
 *         backoffPolicy = BackoffPolicy.EXPONENTIAL,
 *         tags = {"health", "monitoring"}
 *     )
 *     public void healthCheck() { ... }
 * }
 * }</pre>
 *
 * @see JobContext
 * @see RecurringJobBuilder
 * @since 0.1
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Recurring {

  /** Initial backoff delay in milliseconds; actual delay depends on the backoff policy. */
  long backoffDelayMs() default 1000;

  /** The backoff policy to use between retry attempts. */
  BackoffPolicy backoffPolicy() default BackoffPolicy.EXPONENTIAL;

  /**
   * Quartz cron expression: {@code second minute hour day-of-month month day-of-week [year]}.
   * Examples: {@code "0 0 2 * * ?"} (2 AM daily), {@code "0 *\/15 * * * ?"} (every 15 min).
   */
  String cron();

  /** Whether this recurring job is enabled. */
  boolean enabled() default true;

  /**
   * Unique job identifier used as a business key. Defaults to fully-qualified class name + method
   * name if not specified.
   */
  String id() default "";

  /** Maximum number of retry attempts on failure. */
  int maxRetries() default 3;

  /** Display name for monitoring and logs; defaults to the method name if not specified. */
  String name() default "";

  /**
   * Execution priority on a 1-10 scale where 1 is lowest and 10 is highest. Default is 5.
   *
   * <p>Values outside this range cause {@link IllegalArgumentException} during job registration.
   */
  int priority() default 5;

  /** Tags for filtering and categorization. */
  String[] tags() default {};

  /** Maximum execution time in seconds before the job is timed out. Default is 1 hour. */
  long timeoutSeconds() default 3600;

  /** Timezone for the cron expression; must be a valid {@link java.time.ZoneId}. */
  String zone() default "UTC";
}
