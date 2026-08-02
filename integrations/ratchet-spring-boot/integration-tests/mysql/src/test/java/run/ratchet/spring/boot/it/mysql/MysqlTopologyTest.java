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
package run.ratchet.spring.boot.it.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.persistence.EntityManagerFactory;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.annotation.JtaTransactionAnnotationParser;
import run.ratchet.spring.boot.it.mysql.fixture.application.ConsumerNote;
import run.ratchet.spring.boot.it.mysql.fixture.ratchetonly.RatchetOnlyApplication;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.mysql.MysqlJobStore;

class MysqlTopologyTest extends MysqlIntegrationTestSupport {

  @Test
  void createsOneJpaTopologyFromTheInstalledRatchetMappingAndInitializesStoreOnce() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions(""))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertSingleJpaTopology(context);
              assertStoreCoreOrmXmlComesFromInstalledJar();

              EntityManagerFactory entityManagerFactory = entityManagerFactory(context);
              assertThat(entityManagerFactory.getMetamodel().entity(JobEntity.class)).isNotNull();
              assertThat(entityManagerFactory.getMetamodel().entity(NodeEntity.class)).isNotNull();
              assertThat(entityManagerFactory.getMetamodel().getEntities())
                  .noneMatch(entity -> entity.getJavaType().equals(ConsumerNote.class));

              MysqlJobStore store = store(context);
              assertThat(AopUtils.isAopProxy(store)).isTrue();
              Class<?> targetClass = AopUtils.getTargetClass(store);
              Method create = MysqlJobStore.class.getMethod("create", JobEntity.class);
              AnnotationTransactionAttributeSource attributes =
                  new AnnotationTransactionAttributeSource(new JtaTransactionAnnotationParser());
              assertThat(attributes.getTransactionAttribute(create, targetClass)).isNotNull();

              SqlStatementProbe probe = context.getBean(SqlStatementProbe.class);
              assertThat(probe.countContaining("SELECT @@SESSION.transaction_isolation"))
                  .isEqualTo(1L);
            });
  }
}
