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
package run.ratchet.ri.core;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.spi.JobLogger;
import run.ratchet.store.entity.JobLogEntity.LogLevel;

/**
 * Per-job {@link JobLogger} backed by JBoss Logging, with log lines published through {@link
 * InternalEventPublisher} for persistence. Each instance is bound to a single job ID.
 *
 * <p>Created by {@link DefaultJobLoggerFactory} and bound into {@code JobContext} by {@link
 * JobTask}.
 *
 * <p>The {@code isXxxEnabled()} methods return {@code true} when either the underlying logger level
 * is enabled or an {@link InternalEventPublisher} is present. A publisher means the line will be
 * persisted as a job log even when the backend logger would drop that level.
 */
public class JBossLoggingJobLogger implements JobLogger {

  private static final Logger log = Logger.getLogger(JBossLoggingJobLogger.class);

  private final UUID jobId;
  private final InternalEventPublisher eventPublisher;
  private final Clock clock;

  public JBossLoggingJobLogger(UUID jobId, InternalEventPublisher eventPublisher) {
    this(jobId, eventPublisher, Clock.systemUTC());
  }

  public JBossLoggingJobLogger(UUID jobId, InternalEventPublisher eventPublisher, Clock clock) {
    this.jobId = jobId;
    this.eventPublisher = eventPublisher;
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public void info(String message) {
    log.infof("[Job %s] %s", jobId, message);
    publishLogLine(LogLevel.INFO, message);
  }

  @Override
  public void debug(String message) {
    log.debugf("[Job %s] %s", jobId, message);
    publishLogLine(LogLevel.DEBUG, message);
  }

  @Override
  public void warn(String message) {
    log.warnf("[Job %s] %s", jobId, message);
    publishLogLine(LogLevel.WARN, message);
  }

  @Override
  public void error(String message) {
    log.errorf("[Job %s] %s", jobId, message);
    publishLogLine(LogLevel.ERROR, message);
  }

  @Override
  public void trace(String message) {
    log.tracef("[Job %s] %s", jobId, message);
    publishLogLine(LogLevel.TRACE, message);
  }

  @Override
  public boolean isInfoEnabled() {
    return eventPublisher != null || log.isInfoEnabled();
  }

  @Override
  public boolean isDebugEnabled() {
    return eventPublisher != null || log.isDebugEnabled();
  }

  @Override
  public boolean isWarnEnabled() {
    return eventPublisher != null || log.isEnabled(Logger.Level.WARN);
  }

  @Override
  public boolean isErrorEnabled() {
    return eventPublisher != null || log.isEnabled(Logger.Level.ERROR);
  }

  @Override
  public boolean isTraceEnabled() {
    return eventPublisher != null || log.isTraceEnabled();
  }

  private void publishLogLine(LogLevel level, String message) {
    if (eventPublisher != null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> mdcSnapshot =
          MDC.getMap() == null ? new HashMap<>() : new HashMap<>(MDC.getMap());
      eventPublisher.publish(new JobLogLine(jobId, clock.instant(), level, message, mdcSnapshot));
    }
  }
}
