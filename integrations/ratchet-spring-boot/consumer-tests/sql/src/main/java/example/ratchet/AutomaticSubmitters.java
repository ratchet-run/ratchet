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
package example.ratchet;

import static example.ratchet.verification.NativeVerification.track;

import java.io.Serializable;
import run.ratchet.api.JobSchedulerService;

/** Unmanaged application classes: no Ratchet registration annotation or bean definition. */
public class AutomaticSubmitters {
  public static void submit(JobSchedulerService scheduler) {
    track(scheduler.enqueueNow(AutomaticSubmitters::reference));
    String captured = "automatic-capturing";
    track(scheduler.enqueueNow(() -> record(captured)));
    new Nested().submit(scheduler);
    class Local {
      void submit() {
        track(scheduler.enqueueNow(() -> record("automatic-local")));
      }
    }
    new Local().submit();
    new Runnable() {
      public void run() {
        track(scheduler.enqueueNow(() -> record("automatic-anonymous")));
      }
    }.run();
    var argument = new Argument("automatic-record");
    var pojo = new Pojo("automatic-pojo");
    track(scheduler.enqueueNow(() -> recordArgument(argument)));
    track(scheduler.enqueueNow(() -> pojoArgument(pojo)));
  }

  public static void reference() {
    record("automatic-reference");
  }

  public static void record(String value) {
    example.ratchet.verification.NativeVerification.record(value);
  }

  public static void recordArgument(Argument argument) {
    record(argument.value());
  }

  public static void pojoArgument(Pojo argument) {
    record(argument.getValue());
  }

  public record Argument(String value) implements Serializable {}

  public static class Pojo implements Serializable {
    private String value;

    public Pojo() {}

    public Pojo(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  static class Nested {
    void submit(JobSchedulerService scheduler) {
      track(scheduler.enqueueNow(() -> record("automatic-nested")));
    }
  }
}
