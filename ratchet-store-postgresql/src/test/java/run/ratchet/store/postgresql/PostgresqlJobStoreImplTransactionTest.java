package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.tck.store.AbstractJobStoreTransactionBoundaryContract;

class PostgresqlJobStoreImplTransactionTest extends AbstractJobStoreTransactionBoundaryContract {

  @Override
  protected Class<?> jobStoreImplClass() {
    return PostgresqlJobStoreImpl.class;
  }

  @Test
  void readMethodsInheritDefaultTransactionBoundary() throws NoSuchMethodException {
    // Read methods used to carry an explicit @Transactional(SUPPORTS), but on JTA-managed
    // EclipseLink containers (Payara, GlassFish, OpenLiberty) calling a SUPPORTS method outside
    // an outer JTA tx leaked the borrowed pool connection with auto-commit disabled, leaving an
    // open transaction holding metadata locks on subsequent writes. Reverting to the class-level
    // @Transactional default (REQUIRED) makes each read have a clean tx boundary that commits
    // before returning the connection to the pool.
    //
    // getDatabaseTime is the specific read that triggered the original hang -- it runs during
    // startup via DefaultNodeIdentityProvider.checkClockSkew before any outer JTA tx exists.
    assertNoMethodLevelTransactional("getDatabaseTime");
    assertNoMethodLevelTransactional("searchJobs", JobFilter.class, int.class, int.class);
    assertNoMethodLevelTransactional("countJobs", JobFilter.class);
  }

  private static void assertNoMethodLevelTransactional(
      String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
    Transactional annotation =
        PostgresqlJobStoreImpl.class
            .getMethod(methodName, parameterTypes)
            .getAnnotation(Transactional.class);
    assertNull(annotation);
  }
}
