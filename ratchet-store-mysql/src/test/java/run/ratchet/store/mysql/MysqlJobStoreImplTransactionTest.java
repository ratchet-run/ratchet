package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

class MysqlJobStoreImplTransactionTest {

  @Test
  void classLevelTransactionalDefaultsToRequired() {
    // The class-level @Transactional gives every method a default REQUIRED tx boundary unless a
    // method declares its own. Lock the contract here: if the class-level annotation ever gets
    // removed, read methods would silently fall into a no-tx code path on JTA-managed
    // EclipseLink containers and re-introduce the connection leak this test is guarding against.
    Transactional classLevel = MysqlJobStoreImpl.class.getAnnotation(Transactional.class);
    assertNotNull(classLevel);
    assertEquals(Transactional.TxType.REQUIRED, classLevel.value());
  }

  @Test
  void readMethodsInheritDefaultTransactionBoundary() throws NoSuchMethodException {
    // Read methods used to carry an explicit @Transactional(SUPPORTS), but on JTA-managed
    // EclipseLink containers (Payara, GlassFish, OpenLiberty) calling a SUPPORTS method outside
    // an outer JTA tx leaked the borrowed pool connection with auto-commit disabled, leaving an
    // open InnoDB transaction holding metadata locks on subsequent writes. Reverting to the
    // class-level @Transactional default (REQUIRED) makes each read have a clean tx boundary
    // that commits before returning the connection to the pool.
    //
    // getDatabaseTime is the specific read that triggered the original hang — it runs during
    // startup via DefaultNodeIdentityProvider.checkClockSkew before any outer JTA tx exists.
    assertNoMethodLevelTransactional("getDatabaseTime");
    assertNoMethodLevelTransactional("countPendingJobs");
    assertNoMethodLevelTransactional("countActiveNodes");
  }

  private static void assertNoMethodLevelTransactional(
      String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
    Transactional annotation =
        MysqlJobStoreImpl.class
            .getMethod(methodName, parameterTypes)
            .getAnnotation(Transactional.class);
    assertNull(annotation);
  }
}
