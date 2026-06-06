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
package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.api.JobQueryService;
import run.ratchet.tck.api.AbstractJobQueryDenialContract;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.testsuite.app.DenyReadJobAuthorizationPolicy;

/**
 * RI subclass of {@link AbstractJobQueryDenialContract}. The deployment bundles {@link
 * DenyReadJobAuthorizationPolicy} as an enabled {@code @Alternative}, so the injected query service
 * denies reads and scopes list queries.
 */
@ExtendWith(ArquillianExtension.class)
class RiJobQueryDenialIT extends AbstractJobQueryDenialContract {

  @Inject private RiRatchetTckRuntime runtime;
  @Inject private JobQueryService jobQueryService;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  protected JobQueryService deniedQueryService() {
    return jobQueryService;
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.createWith(new Package[0], DenyReadJobAuthorizationPolicy.class);
  }
}
