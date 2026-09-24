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

import static example.ratchet.verification.NativeVerification.*;

import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import run.ratchet.api.*;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.JobInvocation;

@Configuration(proxyBeanMethods = false)
@Profile("verification")
public class SqlNativeLifecycle {
  static final AtomicInteger DESTROYED = new AtomicInteger();

  @Bean
  ExposedTask advisedRecurringTask(Environment environment) {
    if (environment.getProperty("consumer.scenario", "normal").equals("unexposed"))
      return new UnexposedTask();
    return environment.getProperty("consumer.full-verify", Boolean.class, false)
        ? new RecurringTask()
        : new ExposedTask();
  }

  @Bean
  ClassTask advisedClassTask() {
    return new ClassTask();
  }

  @Bean
  @Scope("prototype")
  PrototypeTask advisedPrototypeTask() {
    return new PrototypeTask();
  }

  @Bean
  @org.springframework.core.annotation.Order(-40)
  ApplicationRunner verifyAdvice(
      Environment environment,
      ApplicationContext context,
      InvocationSubmissionService submissions,
      JobQueryService queries) {
    return args -> {
      if (!environment.getProperty("consumer.full-verify", Boolean.class, false)) return;
      check(
          AopUtils.isJdkDynamicProxy(context.getBean("advisedRecurringTask")),
          "missing JDK transaction proxy");
      check(
          AopUtils.isCglibProxy(context.getBean("advisedClassTask")),
          "missing class transaction proxy");
      var singleton =
          submissions
              .enqueueInvocation(
                  new JobInvocation(ClassTask.class.getName(), "run", "()V", false, List.of()))
              .submit();
      var first =
          submissions
              .enqueueInvocation(
                  new JobInvocation(PrototypeTask.class.getName(), "run", "()V", false, List.of()))
              .submit();
      var second =
          submissions
              .enqueueInvocation(
                  new JobInvocation(PrototypeTask.class.getName(), "run", "()V", false, List.of()))
              .submit();
      await(
          () ->
              EXECUTED.contains("advised-recurring")
                  && DESTROYED.get() == 2
                  && List.of(singleton, first, second).stream()
                      .allMatch(job -> hasStatus(queries, job.id(), JobStatus.SUCCEEDED)),
          "advised jobs did not durably complete/release targets");
      var recurring = EXECUTIONS.get("advised-recurring");
      await(
          () -> hasStatus(queries, recurring, JobStatus.SUCCEEDED),
          "advised recurring job did not commit");
      System.out.println("RATCHET_NATIVE_ADVICE_VERIFIED");
    };
  }

  public interface Task {
    void run();
  }

  public static class ExposedTask implements Task {
    @Override
    @Transactional
    public void run() {
      check(
          TransactionSynchronizationManager.isActualTransactionActive(), "JDK advice was bypassed");
      record("advised-recurring");
    }
  }

  public static class RecurringTask extends ExposedTask {
    @Override
    @Recurring(cron = "0/1 * * * * ?", id = "native-advised-recurring")
    public void run() {
      super.run();
    }
  }

  public static class UnexposedTask extends ExposedTask {
    @Recurring(cron = "0/1 * * * * ?", id = "native-unexposed-recurring")
    public void unavailable() {
      throw new AssertionError("unexposed method executed");
    }
  }

  public static class ClassTask {
    @Transactional
    public void run() {
      check(
          TransactionSynchronizationManager.isActualTransactionActive(),
          "class advice was bypassed");
      record("advised-class");
    }
  }

  public static class PrototypeTask implements Task {
    @Override
    @Transactional
    public void run() {
      check(
          TransactionSynchronizationManager.isActualTransactionActive(),
          "prototype advice was bypassed");
      record("advised-prototype");
    }

    @PreDestroy
    public void close() {
      DESTROYED.incrementAndGet();
    }
  }
}
