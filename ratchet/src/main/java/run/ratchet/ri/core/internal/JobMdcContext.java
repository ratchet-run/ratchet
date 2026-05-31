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
package run.ratchet.ri.core.internal;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import org.jboss.logging.MDC;
import run.ratchet.api.JobContext;
import run.ratchet.spi.JobLogger;

/**
 * Binds the per-thread {@link JobContext} for job execution and populates JBoss Logging {@link MDC}
 * keys for log correlation.
 *
 * <h2>Stable contract</h2>
 *
 * <p>The four MDC key names — {@code jobId}, {@code node}, {@code jobCreator}, and {@code jobType}
 * — are part of the public observability surface. Downstream log pipelines, dashboards, and
 * alerting rules may depend on them. Adding new keys is a non-breaking change; renaming or removing
 * one of these four is a breaking change subject to the project's compatibility policy.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>Keys are written on {@link #bindJobContext(UUID, JobLogger, Map, String, String)} and removed
 * (per-key, not via {@code MDC.clear()}) on {@link #clear()}. Per-key removal is deliberate: the
 * enclosing application may have set its own MDC keys (e.g. a request-correlation ID set by a
 * Servlet filter or JAX-RS interceptor) before the job was submitted, and {@code MDC.clear()} would
 * wipe them.
 *
 * <p>{@code bindJobContext(...)} and {@link #clear()} must run on the same execution thread. Both
 * {@link JobContext} and JBoss Logging {@link MDC} use thread-local storage. Code that hands a job
 * to an executor must bind and clear inside the worker thread's try/finally block.
 *
 * <h2>Backend rendering</h2>
 *
 * <p>JBoss Logging is a facade. At runtime it auto-detects an installed backend in this priority
 * order: JBoss LogManager → Log4j 2 → Logback (when {@code ch.qos.logback.classic.Logger} is on the
 * classpath) → JDK {@code java.util.logging}. Whichever provider activates determines whether MDC
 * values render and whether they unify with the application's own MDC entries.
 *
 * <ul>
 *   <li><b>JBoss LogManager</b> (WildFly default) — keys render via {@code %X{jobId}} etc. in
 *       container patterns.
 *   <li><b>Log4j 2</b> — keys render via {@code %X{jobId}} or in JSON layouts via {@code
 *       contextMap}.
 *   <li><b>Logback</b> — keys render via {@code %X{jobId}} or are emitted by {@code JsonEncoder}.
 *       In this configuration, {@link MDC} entries set here and entries set by application code via
 *       {@code org.slf4j.MDC} share the same backend MDC adapter and appear together in output.
 *   <li><b>java.util.logging</b> — keys are stored in JBoss Logging's per-thread map but stock JUL
 *       formatters do not render them. Application code that calls {@code org.slf4j.MDC} via the
 *       {@code slf4j-jdk14} binding writes to a separate map and the two are <em>not</em> unified.
 * </ul>
 *
 * <p>For unified MDC across application and framework code, configure Logback as the runtime
 * backend (place {@code logback-classic} on the classpath; JBoss Logging will detect it
 * automatically). Override the auto-detected provider by setting {@code
 * -Dorg.jboss.logging.provider=slf4j} if the detection priority does not match deployment
 * expectations.
 *
 * @see JobContext
 */
final class JobMdcContext {

  static final String MDC_JOB_ID = "jobId";
  static final String MDC_NODE = "node";
  static final String MDC_JOB_CREATOR = "jobCreator";
  static final String MDC_JOB_TYPE = "jobType";

  private JobMdcContext() {}

  // Entry point for early-load failure paths where node/creator metadata is not yet available.
  static void bindJobContext(UUID jobId, Map<String, String> params) {
    bindJobContext(jobId, NoOpJobLogger.INSTANCE, params, null, null, null);
  }

  static void bindJobContext(
      UUID jobId, Map<String, String> params, String nodeId, String jobCreator) {
    bindJobContext(jobId, NoOpJobLogger.INSTANCE, params, nodeId, jobCreator, null);
  }

  static void bindJobContext(
      UUID jobId,
      JobLogger logger,
      Map<String, String> params,
      String nodeId,
      String jobCreator,
      String jobType) {
    bindJobContext(jobId, logger, params, nodeId, jobCreator, jobType, null);
  }

  static void bindJobContext(
      UUID jobId,
      JobLogger logger,
      Map<String, String> params,
      String nodeId,
      String jobCreator,
      String jobType,
      Serializable signalPayload) {
    JobContext.bind(jobId, logger, params, signalPayload);
    if (jobId != null) {
      MDC.put(MDC_JOB_ID, String.valueOf(jobId));
    }
    if (nodeId != null) {
      MDC.put(MDC_NODE, nodeId);
    }
    if (jobCreator != null) {
      MDC.put(MDC_JOB_CREATOR, jobCreator);
    }
    if (jobType != null) {
      MDC.put(MDC_JOB_TYPE, jobType);
    }
  }

  static void clear() {
    JobContext.clear();
    MDC.remove(MDC_JOB_ID);
    MDC.remove(MDC_NODE);
    MDC.remove(MDC_JOB_CREATOR);
    MDC.remove(MDC_JOB_TYPE);
  }

  private enum NoOpJobLogger implements JobLogger {
    INSTANCE;

    @Override
    public void info(String message) {}

    @Override
    public void debug(String message) {}

    @Override
    public void warn(String message) {}

    @Override
    public void error(String message) {}

    @Override
    public void trace(String message) {}

    @Override
    public boolean isInfoEnabled() {
      return false;
    }

    @Override
    public boolean isDebugEnabled() {
      return false;
    }

    @Override
    public boolean isWarnEnabled() {
      return false;
    }

    @Override
    public boolean isErrorEnabled() {
      return false;
    }

    @Override
    public boolean isTraceEnabled() {
      return false;
    }
  }
}
