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
package run.ratchet.spring.boot.it.aotpreflight;

import org.springframework.stereotype.Component;
import run.ratchet.api.Recurring;

/** Public job targets used by both the JVM control and native preflight. */
@Component
public class AotPreflightJobs {

  public void boundJob() {}

  public static void staticJob() {}

  public static void wrapperJob(
      String value, AotPreflightScenarios.WrapperCapture capturedRecord) {}

  public static String payloadJob(AotPreflightScenarios.PayloadEnvelope payload) {
    return payload.name() + ":" + payload.nested().sequence();
  }

  @Recurring(cron = "0 0 * * * ?")
  public void recurringJob() {}
}
