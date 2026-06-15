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
import run.ratchet.tck.api.AbstractJobQueryContract;
import run.ratchet.tck.api.RatchetTckRuntime;

/** RI subclass of {@link AbstractJobQueryContract} exercising the permit-all read surface. */
@ExtendWith(ArquillianExtension.class)
class RiJobQueryIT extends AbstractJobQueryContract {

  @Inject private RiRatchetTckRuntime runtime;
  @Inject private JobQueryService jobQueryService;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  protected JobQueryService queryService() {
    return jobQueryService;
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create();
  }
}
