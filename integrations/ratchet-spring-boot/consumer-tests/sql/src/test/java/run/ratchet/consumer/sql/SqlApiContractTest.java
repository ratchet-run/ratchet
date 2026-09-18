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
package run.ratchet.consumer.sql;

import example.ratchet.ConsumerApplication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.consumer.SpringApiContracts;
import run.ratchet.tck.api.ListenerProbe;

class SqlApiContractTest extends SpringApiContracts {
  private static SqlDatabase database;
  private static ConfigurableApplicationContext application;

  @BeforeAll
  static void start() {
    database = SqlDatabase.start();
    application =
        new SpringApplicationBuilder(ConsumerApplication.class, ProbeConfiguration.class)
            .properties(database.properties())
            .properties("ratchet.allowed-packages=example.ratchet,run.ratchet.tck.api")
            .run();
  }

  @AfterAll
  static void stop() {
    if (application != null) application.close();
    if (database != null) database.close();
  }

  @Override
  protected ConfigurableApplicationContext context() {
    return application;
  }

  @Override
  protected void resetDatabase() {
    var jdbc = application.getBean(JdbcTemplate.class);
    new TransactionTemplate(application.getBean(PlatformTransactionManager.class))
        .executeWithoutResult(
            status -> {
              for (String table :
                  new String[] {
                    "scheduler_idempotency_key", "scheduler_business_key_reservation",
                        "scheduler_job_queue", "scheduler_job_tag",
                    "scheduler_job_log", "scheduler_job_execution", "scheduler_resource_permit",
                        "scheduler_workflow_condition",
                    "scheduler_batch_metrics", "scheduler_job_archive", "scheduler_job_properties",
                        "scheduler_job_extension_state",
                    "scheduler_job", "scheduler_recurring_job_archive", "scheduler_recurring_job",
                        "scheduler_batch",
                    "scheduler_resource_limit", "scheduler_lock", "scheduler_node"
                  }) jdbc.update("DELETE FROM " + table);
            });
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class ProbeConfiguration {
    @Bean
    ListenerProbe listenerProbe() {
      return new ListenerProbe();
    }
  }
}
