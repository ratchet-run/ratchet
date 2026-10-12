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

import java.time.Duration;
import java.util.function.Predicate;
import org.springframework.jdbc.core.JdbcTemplate;

/** Durable, database-backed fault gates shared by all incarnations. */
public final class Gates {
  public record Hit(String hitId, String point, String jobId, String incarnation, long reachedUs) {}

  private final JdbcTemplate jdbc;

  public Gates(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public void arm(String point, String jobId, int times) {
    if (times <= 0) throw new IllegalArgumentException("times must be positive");
    if (jdbc.update(
            "update chaos_gate_arm set remaining = ? where point = ? and job_id = ?",
            times,
            point,
            jobId)
        == 0) jdbc.update("insert into chaos_gate_arm values (?, ?, ?)", point, jobId, times);
  }

  public Hit awaitHit(String point, String jobId, Predicate<String> incarnation, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      var hits =
          jdbc.query(
              """
              select * from chaos_gate_hit where point = ? and job_id = ?
              order by reached_us, hit_id
              """,
              (rs, row) ->
                  new Hit(
                      rs.getString("hit_id"),
                      rs.getString("point"),
                      rs.getString("job_id"),
                      rs.getString("incarnation"),
                      rs.getLong("reached_us")),
              point,
              jobId);
      for (Hit hit : hits) if (incarnation.test(hit.incarnation())) return hit;
      Thread.sleep(50);
    }
    throw new AssertionError("No matching gate hit: " + point + " " + jobId);
  }

  public void release(String hitId) {
    if (jdbc.update("update chaos_gate_hit set released = 1 where hit_id = ?", hitId) != 1)
      throw new IllegalArgumentException("Unknown gate hit: " + hitId);
  }
}
