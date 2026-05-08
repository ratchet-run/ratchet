package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MysqlJobClaimOperationsTest {

  @Test
  void claimSelectColumnOrderMatchesNamedRowMapping() {
    List<String> columns = MysqlJobClaimOperations.claimSelectColumnNames();

    assertEquals(
        List.of(
            "job_id",
            "status",
            "job_type",
            "priority",
            "scheduled_time",
            "version",
            "timeout_sec",
            "picked_by",
            "picked_at",
            "business_key",
            "attempts",
            "max_retries"),
        columns);
    assertEquals(
        Map.ofEntries(
            Map.entry("job_id", 0),
            Map.entry("status", 1),
            Map.entry("job_type", 2),
            Map.entry("priority", 3),
            Map.entry("scheduled_time", 4),
            Map.entry("version", 5),
            Map.entry("timeout_sec", 6),
            Map.entry("picked_by", 7),
            Map.entry("picked_at", 8),
            Map.entry("business_key", 9),
            Map.entry("attempts", 10),
            Map.entry("max_retries", 11)),
        MysqlJobClaimOperations.claimSelectColumnIndexes());
  }
}
