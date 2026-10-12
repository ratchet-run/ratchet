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

import org.springframework.jdbc.core.JdbcTemplate;

/** Database time shared by node ledgers and fault observations. */
public final class DbClock {
  private final JdbcTemplate jdbc;
  private final String expression;

  public DbClock(JdbcTemplate jdbc, String store) {
    this.jdbc = jdbc;
    expression = expression(store);
  }

  public static String expression(String store) {
    return switch (store) {
      case "postgresql" -> "cast(extract(epoch from clock_timestamp()) * 1000000 as bigint)";
      case "mysql" -> "cast(unix_timestamp(now(6)) * 1000000 as signed)";
      default ->
          throw new IllegalArgumentException(
              "Chaos tests require -Dstore=postgresql or mysql; got " + store);
    };
  }

  public long now() {
    return jdbc.queryForObject("select " + expression, Long.class);
  }
}
