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
package run.ratchet.spring.boot.autoconfigure.jpa;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.transaction.Transactional;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

class RatchetTransactionalStoreProxyTest {

  @Test
  void unannotatedDefaultMethodsReenterConcreteTransactionMetadata() {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:ratchet-proxy-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    JdbcTemplate jdbc = new JdbcTemplate(dataSource);
    jdbc.execute("create table proxy_transaction_test (id varchar(64) primary key)");

    TransactionalDefaultStore proxy = proxy(dataSource, jdbc);

    proxy.writeThroughUnannotatedDefault("required-commit");
    assertThat(
            jdbc.queryForObject(
                "select count(*) from proxy_transaction_test where id = 'required-commit'",
                Integer.class))
        .isEqualTo(1);

    new TransactionTemplate(new DataSourceTransactionManager(dataSource))
        .executeWithoutResult(
            status -> {
              proxy.writeThroughUnannotatedDefault("required-rollback");
              status.setRollbackOnly();
            });
    assertThat(
            jdbc.queryForObject(
                "select count(*) from proxy_transaction_test where id = 'required-rollback'",
                Integer.class))
        .isZero();
  }

  @Test
  void defaultMethodsRetainTheirOwnTransactionMetadataAndReenterConcreteMethods() {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:ratchet-proxy-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    JdbcTemplate jdbc = new JdbcTemplate(dataSource);
    jdbc.execute("create table proxy_transaction_test (id varchar(64) primary key)");

    TransactionalDefaultStore proxy = proxy(dataSource, jdbc);

    new TransactionTemplate(new DataSourceTransactionManager(dataSource))
        .executeWithoutResult(
            status -> {
              proxy.writeThroughDefault("requires-new");
              status.setRollbackOnly();
            });
    assertThat(
            jdbc.queryForObject(
                "select count(*) from proxy_transaction_test where id = 'requires-new'",
                Integer.class))
        .isEqualTo(1);
  }

  private static TransactionalDefaultStore proxy(JdbcDataSource dataSource, JdbcTemplate jdbc) {
    return (TransactionalDefaultStore)
        RatchetTransactionalStoreProxy.createProxy(
            new TransactionalDefaultStoreTarget(jdbc),
            TransactionalDefaultStore.class,
            new DataSourceTransactionManager(dataSource));
  }

  interface TransactionalDefaultStore {

    void write(String id);

    default void writeThroughUnannotatedDefault(String id) {
      write(id);
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    default void writeThroughDefault(String id) {
      write(id);
    }
  }

  private record TransactionalDefaultStoreTarget(JdbcTemplate jdbc)
      implements TransactionalDefaultStore {

    @Override
    @Transactional
    public void write(String id) {
      assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
      jdbc.update("insert into proxy_transaction_test (id) values (?)", id);
    }
  }
}
