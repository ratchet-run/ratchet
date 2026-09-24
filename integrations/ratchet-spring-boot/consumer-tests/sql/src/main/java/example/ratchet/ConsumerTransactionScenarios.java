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

import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import run.ratchet.api.JobHandle;

@Service
public class ConsumerTransactionScenarios {
  private final EntityManager entityManager;
  private final ConsumerService submissions;
  private final ConsumerRequiresNewSubmission requiresNew;

  public ConsumerTransactionScenarios(
      EntityManager entityManager,
      ConsumerService submissions,
      ConsumerRequiresNewSubmission requiresNew) {
    this.entityManager = entityManager;
    this.submissions = submissions;
    this.requiresNew = requiresNew;
  }

  @Transactional
  public JobHandle requiresNewCommitsWhileOuterRollsBack(String outerId, String innerId) {
    entityManager.persist(new ConsumerRecord(outerId, "outer"));
    JobHandle handle = requiresNew.submit(innerId);
    TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
    return handle;
  }

  @Transactional(propagation = Propagation.NOT_SUPPORTED)
  public JobHandle notSupportedSubmitsInItsOwnTransaction(String id) {
    return requiresNew.submit(id);
  }

  @Transactional
  public JobHandle rollbackOnlySuppressesApplicationAndJob(String id) {
    JobHandle handle = submissions.submit(id);
    TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
    return handle;
  }

  @jakarta.transaction.Transactional(rollbackOn = Exception.class)
  public void checkedExceptionRollsBack(String id) throws ConsumerCheckedException {
    submissions.submit(id);
    throw new ConsumerCheckedException();
  }

  public static final class ConsumerCheckedException extends Exception {}
}
