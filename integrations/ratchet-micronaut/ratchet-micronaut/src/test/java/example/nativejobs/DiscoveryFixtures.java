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
package example.nativejobs;

import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.Recurring;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;

/** Compiled bytecode fixtures, kept outside the excluded Ratchet runtime namespaces. */
public final class DiscoveryFixtures {
  private DiscoveryFixtures() {}

  public record Order(String number) {}

  public static class Pojo {
    public String value;

    public Pojo() {}
  }

  public interface TargetContract {
    void ping();
  }

  public static class Target implements TargetContract {
    public void ping() {}

    public Order order(Order[] value) {
      return value[0];
    }

    public void pojo(Pojo value) {}

    public static void staticCall(String value) {}
  }

  public static class MethodReferenceSubmitter {
    public SerializableCheckedRunnable job(Target target) {
      return target::ping;
    }

    public void submit(JobSchedulerService scheduler, Target target) {
      scheduler.enqueue(target::ping).submit();
    }

    public SerializableConsumer<Pojo> argument(Target target) {
      return target::pojo;
    }
  }

  public static class CapturingSubmitter {
    public SerializableCheckedRunnable job(Target target, Order[] orders) {
      return () -> target.order(orders);
    }
  }

  public static class StaticSubmitter {
    public SerializableCheckedRunnable job(String value) {
      return () -> Target.staticCall(value);
    }
  }

  public static class ParentSubmitter {
    public SerializableCheckedRunnable job(Target target) {
      return target::ping;
    }

    public void submit(JobSchedulerService scheduler, Target target) {
      scheduler.enqueue(target::ping).submit();
    }
  }

  public static class InheritedSubmitter extends ParentSubmitter {}

  public static class EnclosingSubmitter {
    public void submit(JobSchedulerService scheduler, Target target) {
      scheduler.enqueue(target::ping).submit();
    }

    public static class NonSubmittingNested {
      public void helper() {}
    }

    public static class Nested {
      public SerializableCheckedRunnable job(Target target) {
        return target::ping;
      }

      public void submit(JobSchedulerService scheduler, Target target) {
        scheduler.enqueue(target::ping).submit();
      }
    }

    public Object local(Target target) {
      class Local {
        public SerializableCheckedRunnable job() {
          return target::ping;
        }
      }
      return new Local();
    }

    public Object anonymous(Target target) {
      return new Object() {
        public SerializableCheckedRunnable job() {
          return target::ping;
        }
      };
    }
  }

  public static class RecurringBean {
    @Recurring(cron = "0 * * * * ?")
    public void tick() {}
  }

  public static class Unrelated {
    public void ignored() {}
  }
}
