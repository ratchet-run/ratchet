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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import run.ratchet.spi.JobLogger;

/** Thread-local context for the executing job. */
public final class JobContext {

  private static final ThreadLocal<JobContext> TL = new ThreadLocal<>();

  private final UUID jobId;
  private final JobLogger logger;
  private final Map<String, String> params;
  private final Serializable signalPayload;

  private JobContext(UUID jobId, JobLogger logger) {
    this(jobId, logger, Collections.emptyMap(), null);
  }

  private JobContext(UUID jobId, JobLogger logger, Map<String, String> params) {
    this(jobId, logger, params, null);
  }

  private JobContext(
      UUID jobId, JobLogger logger, Map<String, String> params, Serializable signalPayload) {
    this.jobId = jobId;
    this.logger = logger;
    this.params = params != null ? Collections.unmodifiableMap(params) : Collections.emptyMap();
    this.signalPayload = signalPayload;
  }

  /**
   * Binds a new context to the current thread. Always pair with {@link #clear()} in a finally
   * block.
   */
  public static JobContext bind(UUID jobId, JobLogger logger) {
    JobContext ctx = new JobContext(jobId, logger);
    TL.set(ctx);
    return ctx;
  }

  /**
   * Binds a new context with parameters to the current thread. Always pair with {@link #clear()} in
   * a finally block.
   */
  public static JobContext bind(UUID jobId, JobLogger logger, Map<String, String> params) {
    JobContext ctx = new JobContext(jobId, logger, params);
    TL.set(ctx);
    return ctx;
  }

  /**
   * Binds a new context with parameters and a pre-deserialized signal payload. Called by the job
   * executor for signal-waiting jobs; the payload is deserialized before bind so {@code JobContext}
   * carries no serializer dependency. Always pair with {@link #clear()} in a finally block.
   */
  public static JobContext bind(
      UUID jobId, JobLogger logger, Map<String, String> params, Serializable signalPayload) {
    JobContext ctx = new JobContext(jobId, logger, params, signalPayload);
    TL.set(ctx);
    return ctx;
  }

  /** Removes the context bound to the current thread. */
  public static void clear() {
    TL.remove();
  }

  /**
   * @return the context bound to the current thread
   * @throws IllegalStateException if no context is bound
   */
  public static JobContext current() {
    JobContext ctx = TL.get();
    if (ctx == null) {
      throw new IllegalStateException("No JobContext bound to current thread");
    }
    return ctx;
  }

  /** Returns the UUID of the executing job. */
  public UUID jobId() {
    return jobId;
  }

  /** Returns the job-scoped logger (automatically includes job ID in all entries). */
  public JobLogger logger() {
    return logger;
  }

  /**
   * @return the parameter value, or null if the key does not exist
   */
  public String param(String key) {
    return params.get(key);
  }

  /**
   * @return the parameter value if present, otherwise {@code defaultValue}
   */
  public String param(String key, String defaultValue) {
    return params.getOrDefault(key, defaultValue);
  }

  /**
   * Returns an unmodifiable map of job parameters.
   *
   * @return job parameters, or an empty map if none were supplied
   */
  public Map<String, String> params() {
    return params;
  }

  /**
   * Returns the signal payload delivered to this job, cast to the requested type, or {@code null}
   * if this job was not a signal-waiting job or no payload was included with the signal.
   *
   * @throws ClassCastException if the payload cannot be cast to {@code type}
   */
  @SuppressWarnings("unchecked")
  public <T extends Serializable> T signalPayload(Class<T> type) {
    return signalPayload == null ? null : type.cast(signalPayload);
  }
}
