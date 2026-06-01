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
package run.ratchet.showcase.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.showcase.domain.GeneratedOrder;
import run.ratchet.showcase.domain.StreamState;
import run.ratchet.showcase.jobs.ShowcaseJobs;

@ApplicationScoped
public class OrderStreamService {

  public static final String STREAM_BUSINESS_KEY = "showcase-order-stream";

  @Inject JobSchedulerService scheduler;
  @Inject OrderRepository repository;
  @Inject SeededOrderGenerator generator;
  @Inject OrderScenarioService scenarios;

  public StreamState start(
      Long seed, Integer ordersPerMinute, Double burstiness, Double failureMix) {
    long effectiveSeed = seed == null ? SeededOrderGenerator.DEFAULT_SEED : seed;
    int effectiveRate = ordersPerMinute == null ? 30 : ordersPerMinute;
    double effectiveBurstiness = burstiness == null ? 0.25 : burstiness;
    double effectiveFailureMix = failureMix == null ? 0.35 : failureMix;
    scheduler.cancelRecurringJobByBusinessKey(STREAM_BUSINESS_KEY);
    repository.startStream(effectiveSeed, effectiveRate, effectiveBurstiness, effectiveFailureMix);
    scheduler
        .scheduleRecurringUtc("*/1 * * * * ?", ShowcaseJobs::produceOrderStream)
        .withBusinessKey(STREAM_BUSINESS_KEY)
        .withTags(List.of(ShowcaseTags.SHOWCASE, ShowcaseTags.STREAM))
        .withOptions(JobOptions.defaults().withPriority(JobPriority.HIGH).withMaxRetries(3))
        .submit();
    return repository.streamState();
  }

  public StreamState update(
      Integer ordersPerMinute, Long seed, Double burstiness, Double failureMix) {
    repository.updateStream(ordersPerMinute, seed, burstiness, failureMix);
    return repository.streamState();
  }

  public StreamState stop() {
    scheduler.cancelRecurringJobByBusinessKey(STREAM_BUSINESS_KEY);
    repository.stopStream();
    return repository.streamState();
  }

  public synchronized int produceDueOrders() {
    StreamState state = repository.streamState();
    int due = repository.drainDueOrders();
    int submitted = 0;
    for (int i = 0; i < due; i++) {
      long sequence = repository.nextSequence();
      GeneratedOrder order = generator.generate(state.seed, sequence, state.failureMix);
      JobHandle ignored = scenarios.submitGeneratedOrder(order, ShowcaseTags.STREAM);
      submitted++;
    }
    return submitted;
  }
}
