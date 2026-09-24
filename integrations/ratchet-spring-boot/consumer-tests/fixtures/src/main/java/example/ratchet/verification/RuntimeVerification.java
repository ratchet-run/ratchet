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

import static example.ratchet.verification.NativeVerification.check;

import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.migration.SchemaInitializationException;

@Configuration(proxyBeanMethods = false)
public class RuntimeVerification {
  static final AtomicInteger HOOK_DESTROYED = new AtomicInteger();
  static final AtomicInteger HOOK_STOPPED = new AtomicInteger();
  static final CountDownLatch ENTERED = new CountDownLatch(1);
  static final CountDownLatch RELEASE = new CountDownLatch(1);
  static volatile boolean completed;
  static volatile boolean interrupted;

  @Bean
  @Scope("prototype")
  CompletedHook completedVerificationHook() {
    return new CompletedHook();
  }

  @Bean
  FailedHook failingVerificationHook(Environment environment) {
    return new FailedHook(environment);
  }

  @Bean
  DrainJob verificationDrainJob() {
    return new DrainJob();
  }

  @Bean
  @org.springframework.core.annotation.Order(-30)
  ApplicationRunner verifyDrain(
      Environment environment, JobSchedulerService scheduler, DrainJob job) {
    return args -> {
      if (!environment.getProperty("consumer.scenario", "normal").equals("drain")) return;
      scheduler.enqueue(job::run).withBusinessKey("native-drain").submit();
      check(ENTERED.await(30, TimeUnit.SECONDS), "drain job never entered");
    };
  }

  @Priority(1)
  public static class CompletedHook implements SchedulerLifecycleHook {
    @Override
    public void afterStop() {
      HOOK_STOPPED.incrementAndGet();
    }

    @PreDestroy
    public void destroy() {
      HOOK_DESTROYED.incrementAndGet();
    }
  }

  @Priority(2)
  public static class FailedHook implements SchedulerLifecycleHook {
    private final Environment environment;

    FailedHook(Environment environment) {
      this.environment = environment;
    }

    @Override
    public void beforeStart() {
      if (environment.getProperty("consumer.scenario", "normal").equals("startup-failure"))
        throw new SchemaInitializationException("native intentional startup failure");
    }
  }

  public static class DrainJob {
    public void run() throws InterruptedException {
      ENTERED.countDown();
      try {
        check(RELEASE.await(40, TimeUnit.SECONDS), "drain job was not released");
        completed = true;
      } catch (InterruptedException failure) {
        interrupted = true;
        throw failure;
      }
    }
  }

  /** Failure scenarios restart in this same process to detect leaked runtime ownership. */
  public static void launch(Class<?> application, String[] args) throws Exception {
    String scenario =
        Arrays.stream(args)
            .filter(value -> value.startsWith("--consumer.scenario="))
            .map(value -> value.substring(value.indexOf('=') + 1))
            .findFirst()
            .orElse("normal");
    boolean failureExpected = scenario.equals("startup-failure") || scenario.equals("unexposed");
    ConfigurableApplicationContext context;
    try {
      context = SpringApplication.run(application, args);
    } catch (RuntimeException failure) {
      if (!failureExpected) throw failure;
      String expected =
          scenario.equals("startup-failure")
              ? "native intentional startup failure"
              : "cannot invoke @Recurring method through its Spring proxy";
      boolean found = false;
      for (Throwable cause = failure; cause != null; cause = cause.getCause())
        if (cause.getMessage() != null && cause.getMessage().contains(expected)) found = true;
      check(found, "unexpected startup failure: " + failure);
      if (scenario.equals("startup-failure")) {
        check(HOOK_DESTROYED.get() == 1, "prototype hook was not destroyed exactly once");
        check(HOOK_STOPPED.get() == 0, "stop callback ran for incomplete startup");
      }
      String[] restart =
          Arrays.stream(args)
              .filter(value -> !value.startsWith("--consumer.scenario="))
              .toArray(String[]::new);
      context = SpringApplication.run(application, restart);
      context.close();
      System.out.println("RATCHET_STARTUP_RECOVERY_VERIFIED " + scenario);
      return;
    }
    if (failureExpected) {
      context.close();
      throw new IllegalStateException("invalid startup configuration was accepted: " + scenario);
    }
    if (scenario.equals("drain")) {
      DrainController drain = context.getBean(DrainController.class);
      ConfigurableApplicationContext closingContext = context;
      var closing = CompletableFuture.runAsync(closingContext::close);
      try {
        NativeVerification.await(drain::isDraining, "shutdown did not start draining");
        check(!closing.isDone(), "shutdown returned while work was blocked");
        check(!interrupted, "graceful shutdown interrupted running work");
      } finally {
        RELEASE.countDown();
      }
      closing.get(30, TimeUnit.SECONDS);
      check(completed && !interrupted, "shutdown failed to drain running work");
      System.out.println("RATCHET_RUNNING_SHUTDOWN_VERIFIED");
    } else if (context.getEnvironment().getProperty("consumer.verify", Boolean.class, false)) {
      context.close();
    }
  }
}
