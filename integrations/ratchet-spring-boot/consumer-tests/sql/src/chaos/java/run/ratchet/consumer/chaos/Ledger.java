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
package run.ratchet.consumer.chaos;

import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

/** Durable attempt and terminal-event observations for one run. */
public record Ledger(List<Attempt> attempts, List<Commit> commits) {
  public record Attempt(
      String attemptId,
      String runId,
      String key,
      String jobId,
      String nodeId,
      String incarnation,
      long startedUs,
      Long finishedUs,
      String outcome) {}

  public record Commit(
      String jobId, String nodeId, String incarnation, String terminalStatus, long committedUs) {}

  public static Ledger load(JdbcTemplate jdbc, String run, String store) {
    var attempts =
        jdbc.query(
            "select * from chaos_attempt where run_id = ? order by started_us, attempt_id",
            (rs, row) ->
                new Attempt(
                    rs.getString("attempt_id"),
                    rs.getString("run_id"),
                    rs.getString("job_key"),
                    rs.getString("job_id"),
                    rs.getString("node_id"),
                    rs.getString("incarnation"),
                    rs.getLong("started_us"),
                    rs.getObject("finished_us", Long.class),
                    rs.getString("outcome")),
            run);
    String id = store.equals("mysql") ? "BIN_TO_UUID(t.job_id)" : "cast(t.job_id as varchar(36))";
    var commits =
        jdbc.query(
            "select c.* from chaos_commit c join scheduler_job_tag t on "
                + id
                + " = c.job_id where t.tag = ?",
            (rs, row) ->
                new Commit(
                    rs.getString("job_id"),
                    rs.getString("node_id"),
                    rs.getString("incarnation"),
                    rs.getString("terminal_status"),
                    rs.getLong("committed_us")),
            "run:" + run);
    return new Ledger(attempts, commits);
  }
}
