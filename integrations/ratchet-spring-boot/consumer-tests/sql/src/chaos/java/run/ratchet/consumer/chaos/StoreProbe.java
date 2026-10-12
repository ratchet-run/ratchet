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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.jdbc.core.JdbcTemplate;

/** Plain SQL observations of the cold jobs and cluster ownership tables. */
public final class StoreProbe {
  private final JdbcTemplate jdbc;
  private final String id;
  private final String now;

  public StoreProbe(JdbcTemplate jdbc, String store) {
    this.jdbc = jdbc;
    DbClock.expression(store);
    id = store.equals("mysql") ? "BIN_TO_UUID(j.job_id)" : "cast(j.job_id as varchar(36))";
    now = store.equals("mysql") ? "now(6)" : "clock_timestamp()";
  }

  public Map<String, String> statuses(String run) {
    var result = new LinkedHashMap<String, String>();
    jdbc.query(
        "select "
            + id
            + " as id, j.terminal_status from scheduler_job j join scheduler_job_tag t on t.job_id"
            + " = j.job_id where t.tag = ?",
        rs -> {
          result.put(rs.getString("id"), rs.getString("terminal_status"));
        },
        "run:" + run);
    return result;
  }

  public List<String> quiescence(Set<String> live) {
    var evidence = new ArrayList<String>();
    int running =
        jdbc.queryForObject(
            "select count(*) from scheduler_job_queue where status = 'RUNNING'", Integer.class);
    int permits =
        jdbc.queryForObject("select count(*) from scheduler_resource_permit", Integer.class);
    if (running != 0) evidence.add("RUNNING rows: " + running);
    if (permits != 0) evidence.add("resource permit rows: " + permits);
    for (String node : jdbc.queryForList("select node_id from scheduler_node", String.class))
      if (!live.contains(node)) evidence.add("dead node row: " + node);
    for (String owner :
        jdbc.queryForList(
            "select owner_node from scheduler_lock where expires_at > " + now, String.class))
      if (!live.contains(owner)) evidence.add("unexpired lock owned by: " + owner);
    return evidence;
  }
}
