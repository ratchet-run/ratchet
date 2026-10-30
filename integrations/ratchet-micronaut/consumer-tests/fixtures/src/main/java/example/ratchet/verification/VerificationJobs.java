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
package example.ratchet.verification;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.Recurring;

@Singleton
@Requires(property = "consumer.native-check", value = "true")
public class VerificationJobs {
  public static final Set<String> EXECUTED = ConcurrentHashMap.newKeySet();
  public static final AtomicInteger TICKS = new AtomicInteger();

  public void reference() {
    EXECUTED.add("METHOD_REFERENCE");
  }

  public void accept(String value) {
    EXECUTED.add(value);
  }

  public static void staticTarget(String value) {
    EXECUTED.add(value);
  }

  public void record(RecordArgument value) {
    EXECUTED.add(value.value());
  }

  public void pojo(PojoArgument value) {
    EXECUTED.add(value.getValue());
  }

  public void durable(String value) throws InterruptedException {
    System.out.println("RATCHET_JOB_STARTED " + value);
    System.out.flush();
    if (Boolean.getBoolean("consumer.block")) {
      while (true) Thread.sleep(1000);
    }
    if (Boolean.getBoolean("consumer.drain")) Thread.sleep(5000);
    EXECUTED.add(value);
    System.out.println("RATCHET_JOB_COMPLETED " + value);
    System.out.flush();
  }

  @Recurring(cron = "0/1 * * * * ?", id = "native-verification-recurring")
  public void recurring() {
    TICKS.incrementAndGet();
  }

  public record RecordArgument(String value) {}

  public static class PojoArgument {
    private String value;

    public PojoArgument() {}

    public PojoArgument(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }
}
