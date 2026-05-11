package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractResourcePermitStoreContract;

class PostgresqlResourcePermitStoreContractTest extends AbstractResourcePermitStoreContract {

  private final PostgresqlTestFixture fixture = new PostgresqlTestFixture();

  @Override
  public JobStore store() {
    return fixture.store();
  }

  @Override
  public JobEntity newPendingJob() {
    return fixture.newPendingJob();
  }

  @Override
  public JobEntity newBatchParentJob() {
    return fixture.newBatchParentJob();
  }

  @Override
  public void cleanupStore() {
    fixture.cleanupStore();
  }

  @Test
  void tryAcquirePermit_usesCapacityGatedInsert() throws IOException {
    String source =
        Files.readString(
            Path.of(
                "src/main/java/run/ratchet/store/postgresql/PostgresqlAuxiliaryOperations.java"));
    int start = source.indexOf("public boolean tryAcquirePermit");
    int end = source.indexOf("public void releasePermit");
    assertTrue(start >= 0, "tryAcquirePermit method should exist in source");
    assertTrue(end > start, "releasePermit should appear after tryAcquirePermit in source");
    String method = source.substring(start, end);

    assertTrue(
        method.contains("INSERT INTO scheduler_resource_permit")
            && method.contains(") < max_concurrent")
            && !method.contains("String countSql"),
        "tryAcquirePermit must gate permit insertion with the capacity check");
  }
}
