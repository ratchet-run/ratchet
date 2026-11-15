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
package run.ratchet.spring.boot.autoconfigure;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.exception.DuplicateBusinessKeyException;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.ri.core.DefaultJobCreationService;
import run.ratchet.ri.core.JobTransaction;

class SpringJobTransactionTest {
  @Test
  void nativeJakartaParserPreservesBothSubmissionConflicts() throws Exception {
    var method = DefaultJobCreationService.class.getMethod("submit", JobBuilder.class);
    var attribute =
        new AnnotationTransactionAttributeSource()
            .getTransactionAttribute(method, DefaultJobCreationService.class);
    assertNotNull(attribute);
    assertFalse(attribute.rollbackOn(new DuplicateIdempotencyKeyException("key")));
    assertFalse(attribute.rollbackOn(new DuplicateBusinessKeyException("key", "held")));
    assertTrue(attribute.rollbackOn(new IllegalStateException("failure")));
  }

  @Test
  void templateUsesOneSelectedTransactionAndAvoidsAutomaticProxy() throws Exception {
    try (var context = new AnnotationConfigApplicationContext(Config.class)) {
      JobTransaction carrier = context.getBean(JobTransaction.class);
      assertFalse(AopUtils.isAopProxy(carrier));
      assertEquals(
          "done",
          carrier.run(
              () -> {
                assertTrue(JobTransaction.active());
                return "done";
              }));
      PlatformTransactionManager manager = context.getBean(PlatformTransactionManager.class);
      verify(manager)
          .getTransaction(
              argThat(
                  definition ->
                      definition.getPropagationBehavior()
                          == TransactionDefinition.PROPAGATION_REQUIRED));
      verify(manager).commit(any());
      verify(manager, never()).rollback(any());
      assertFalse(JobTransaction.active());
    }
  }

  @Test
  void checkedFailureRollsBackAndPropagatesUnwrapped() {
    PlatformTransactionManager manager = mock(PlatformTransactionManager.class);
    when(manager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));
    IOException failure = new IOException("body");
    assertSame(
        failure,
        assertThrows(
            IOException.class,
            () ->
                new SpringJobTransaction(manager)
                    .run(
                        () -> {
                          throw failure;
                        })));
    verify(manager).rollback(any());
    verify(manager, never()).commit(any());
    assertFalse(JobTransaction.active());
  }

  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement
  static class Config {
    @Bean
    PlatformTransactionManager manager() {
      PlatformTransactionManager manager = mock(PlatformTransactionManager.class);
      when(manager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));
      return manager;
    }

    @Bean
    SpringJobTransaction carrier(PlatformTransactionManager manager) {
      return new SpringJobTransaction(manager);
    }
  }
}
