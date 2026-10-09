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

import static example.ratchet.verification.NativeVerification.await;
import static example.ratchet.verification.NativeVerification.check;

import example.ratchet.CircuitBreakerBeanConfiguration.ProtectedService;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.SignalStore;

/** Transaction and host-persistence assertions run inside the AOT application/native process. */
@Configuration(proxyBeanMethods = false)
@Profile("verification")
public class SqlNativeVerification {
  @Bean
  @Order(-50)
  ApplicationRunner verifySqlNative(
      Environment environment,
      ConsumerTransactionScenarios scenarios,
      ProtectedService protectedService,
      ConsumerService service,
      ConsumerHostDataService host,
      JdbcTemplate jdbc,
      PlatformTransactionManager transactions,
      AfterCommitRegistrar afterCommit,
      JobStore store,
      JobQueryService queries) {
    return args -> {
      if (!environment.getProperty("consumer.full-verify", Boolean.class, false)) return;
      check(AopUtils.isAopProxy(protectedService), "circuit-breaker bean has no native proxy");
      for (int i = 0; i < 3; i++) {
        try {
          protectedService.call(true);
          throw new AssertionError("protected call did not fail");
        } catch (IllegalStateException expected) {
          // Recorded failures must open the same application-bean breaker in native code.
        }
      }
      try {
        protectedService.call(false);
        throw new AssertionError("open native circuit admitted call");
      } catch (CircuitBreakerOpenException expected) {
        check(protectedService.calls() == 3, "open native circuit reached target");
      }
      check(
          store.capability(SignalStore.class).orElseThrow() == store,
          "capability lookup dropped the transaction proxy");
      var rolledBack = scenarios.rollbackOnlySuppressesApplicationAndJob("native-rollback");
      check(queries.getJobDetail(rolledBack.id()).isEmpty(), "job survived application rollback");
      check(
          jdbc.queryForObject(
                  "select count(*) from consumer_record where id = ?",
                  Long.class,
                  "native-rollback")
              == 0,
          "application rollback");
      scenarios.requiresNewCommitsWhileOuterRollsBack("native-outer", "native-inner");
      check(
          jdbc.queryForObject(
                  "select count(*) from consumer_record where id = ?", Long.class, "native-outer")
              == 0,
          "outer transaction committed");
      await(
          () ->
              "executed"
                  .equals(
                      jdbc.queryForObject(
                          "select state from consumer_record where id = ?",
                          String.class,
                          "native-inner")),
          "REQUIRES_NEW job did not execute");
      AtomicBoolean callback = new AtomicBoolean();
      new TransactionTemplate(transactions)
          .executeWithoutResult(
              status -> {
                check(
                    afterCommit.registerAfterCommit(
                            () -> {
                              service.submit("native-after-commit");
                              callback.set(true);
                            })
                        == AfterCommitRegistrar.Result.REGISTERED,
                    "after-commit not registered");
                check(!callback.get(), "after-commit executed inside the transaction");
              });
      await(
          () ->
              "executed"
                  .equals(
                      jdbc.queryForObject(
                          "select state from consumer_record where id = ?",
                          String.class,
                          "native-after-commit")),
          "after-commit submission did not execute");
      AtomicBoolean rollbackCallback = new AtomicBoolean();
      new TransactionTemplate(transactions)
          .executeWithoutResult(
              status -> {
                afterCommit.registerAfterCommit(() -> rollbackCallback.set(true));
                status.setRollbackOnly();
              });
      check(!rollbackCallback.get(), "rollback ran after-commit action");
      UUID id = UUID.randomUUID();
      Instant time = Instant.parse("2026-01-02T03:04:05.123456Z");
      host.save(id, new ConsumerLabel("native-host"), time);
      ConsumerUuidRecord roundTrip = host.find(id);
      check(
          id.equals(roundTrip.getId())
              && time.equals(roundTrip.getOccurredAt())
              && new ConsumerLabel("native-host").equals(roundTrip.getLabel()),
          "host entity/converter UUID/time roundtrip");
      System.out.println("RATCHET_SQL_NATIVE_VERIFIED");
    };
  }
}
