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

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.transaction.hibernate.HibernateTransactionManager;
import jakarta.inject.Singleton;
import run.ratchet.api.TransactionalJob;

/** Jobs that return with their transaction marked for rollback. */
@Singleton
@Requires(property = "consumer.transactional-jobs", value = "true")
public class RollbackOnlyJobs {
  private final BeanContext beans;

  public RollbackOnlyJobs(BeanContext beans) {
    this.beans = beans;
  }

  @TransactionalJob
  public void execute(String id, String scenario) {
    beans
        .getBean(HibernateTransactionManager.class, Qualifiers.byName("default"))
        .getConnection()
        .createNativeQuery("INSERT INTO consumer_record (id, state) VALUES (?1, ?2)")
        .setParameter(1, id)
        .setParameter(2, "unit")
        .executeUpdate();
    if (scenario.equals("nested")) {
      try {
        beans.getBean(ConsumerNestedFailure.class).fail();
      } catch (RuntimeException expected) {
        // The job carries on with a transaction marked for rollback.
      }
    }
    if (scenario.equals("rollbackOnly")) {
      beans
          .getBean(HibernateTransactionManager.class, Qualifiers.byName("default"))
          .findTransactionStatus()
          .orElseThrow()
          .setRollbackOnly();
    }
  }
}
