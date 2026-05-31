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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Objects;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.spi.JobLogger;
import run.ratchet.spi.JobLoggerContext;
import run.ratchet.spi.JobLoggerFactory;

/** Default per-job logger factory backed by JBoss Logging and Ratchet log events. */
@ApplicationScoped
class DefaultJobLoggerFactory implements JobLoggerFactory {

  private final InternalEventPublisher eventPublisher;
  private final Clock clock;

  protected DefaultJobLoggerFactory() {
    this.eventPublisher = null;
    this.clock = null;
  }

  public DefaultJobLoggerFactory(InternalEventPublisher eventPublisher) {
    this(eventPublisher, Clock.systemUTC());
  }

  @Inject
  public DefaultJobLoggerFactory(InternalEventPublisher eventPublisher, Clock clock) {
    this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public JobLogger create(JobLoggerContext context) {
    if (eventPublisher == null) {
      throw new IllegalStateException(
          "DefaultJobLoggerFactory requires an injected InternalEventPublisher");
    }
    return new JBossLoggingJobLogger(context.jobId(), eventPublisher, clock);
  }
}
