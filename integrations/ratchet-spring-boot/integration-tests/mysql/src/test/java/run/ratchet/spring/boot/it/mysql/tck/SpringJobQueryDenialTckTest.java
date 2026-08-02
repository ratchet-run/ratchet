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
package run.ratchet.spring.boot.it.mysql.tck;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spring.boot.it.mysql.fixture.tck.SpringRatchetTckRuntime;
import run.ratchet.tck.api.AbstractJobQueryDenialContract;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/** Spring MySQL binding for {@link AbstractJobQueryDenialContract}. */
@SpringMysqlTck
@Import(SpringJobQueryDenialTckTest.DenyReadConfiguration.class)
class SpringJobQueryDenialTckTest extends AbstractJobQueryDenialContract {

  @Autowired SpringRatchetTckRuntime runtime;
  @Autowired JobQueryService jobQueryService;

  @BeforeEach
  void clearBeforeEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  protected JobQueryService deniedQueryService() {
    return jobQueryService;
  }

  @Configuration(proxyBeanMethods = false)
  static class DenyReadConfiguration {

    @Bean
    @Primary
    JobAuthorizationPolicy denyReadJobAuthorizationPolicy() {
      return new DenyReadJobAuthorizationPolicy();
    }
  }
}

final class DenyReadJobAuthorizationPolicy implements JobAuthorizationPolicy {

  private static final String UNMATCHABLE_PRINCIPAL = "__denied-no-match__";

  @Override
  public void checkCreate(UUID jobId, String callerPrincipal) throws JobAuthorizationException {}

  @Override
  public void checkRead(UUID jobId, String callerPrincipal) throws JobAuthorizationException {
    throw new JobAuthorizationException(jobId, "read", callerPrincipal, "Read denied by policy");
  }

  @Override
  public JobFilter filterForPrincipal(JobFilter filter, String callerPrincipal) {
    return filter.toBuilder().callerPrincipal(UNMATCHABLE_PRINCIPAL).build();
  }

  @Override
  public void checkCancel(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  @Override
  public void checkPause(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  @Override
  public void checkResume(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  @Override
  public void checkRetry(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}
}
