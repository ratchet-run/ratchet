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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.store.AbstractJobStoreTransactionBoundaryContract;

class OracleJobStoreImplTransactionTest extends AbstractJobStoreTransactionBoundaryContract {

  @Override
  protected Class<?> jobStoreImplClass() {
    return OracleJobStoreImpl.class;
  }

  @Test
  void readMethodsInheritDefaultTransactionBoundary() throws NoSuchMethodException {
    // Read methods used to carry an explicit @Transactional(SUPPORTS), but on JTA-managed
    // EclipseLink containers (Payara, GlassFish, OpenLiberty) calling a SUPPORTS method outside
    // an outer JTA tx leaked the borrowed pool connection with auto-commit disabled, leaving an
    // open Oracle transaction holding table locks on subsequent writes. Reverting to the
    // class-level @Transactional default (REQUIRED) makes each read have a clean tx boundary
    // that commits before returning the connection to the pool.
    //
    // getDatabaseTime is the specific read that triggered the original hang -- it runs during
    // startup via DefaultNodeIdentityProvider.checkClockSkew before any outer JTA tx exists.
    assertNoMethodLevelTransactional("getDatabaseTime");
    assertNoMethodLevelTransactional("countPendingJobs");
    assertNoMethodLevelTransactional("countActiveNodes");
  }

  private static void assertNoMethodLevelTransactional(
      String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
    Transactional annotation =
        OracleJobStoreImpl.class
            .getMethod(methodName, parameterTypes)
            .getAnnotation(Transactional.class);
    assertNull(annotation);
  }
}
