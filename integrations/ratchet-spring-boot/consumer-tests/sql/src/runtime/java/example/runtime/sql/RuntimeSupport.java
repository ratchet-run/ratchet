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
package example.runtime.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import example.ratchet.ConsumerApplication;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobStatus;
import run.ratchet.consumer.sql.SqlDatabase;

public final class RuntimeSupport {
  static Map<String, Object> properties(SqlDatabase database) {
    if (!database.store().equals("postgresql"))
      throw new IllegalArgumentException(
          "The runtime SQL scenarios require -Dstore=postgresql; use the standard consumer suite for the other vendors");
    var properties = new LinkedHashMap<>(database.properties());
    properties.put("spring.flyway.enabled", false);
    properties.put("spring.liquibase.enabled", false);
    properties.put("ratchet.allowed-packages", "example.ratchet,example.runtime");
    properties.put("ratchet.poller.min-delay-ms", 100);
    properties.put("ratchet.poller.max-delay-ms", 500);
    return properties;
  }

  static ConfigurableApplicationContext start(SqlDatabase database, Class<?>... sources) {
    return new SpringApplicationBuilder(ConsumerApplication.class)
        .sources(sources)
        .properties(properties(database))
        .run();
  }

  static DataSource dataSource(SqlDatabase database) {
    var properties = database.properties();
    return new DriverManagerDataSource(
        (String) properties.get("spring.datasource.url"),
        (String) properties.get("spring.datasource.username"),
        (String) properties.get("spring.datasource.password"));
  }

  static void status(ConfigurableApplicationContext context, JobHandle handle, JobStatus expected) {
    await()
        .atMost(Duration.ofSeconds(40))
        .untilAsserted(
            () ->
                assertThat(
                        context
                            .getBean(JobQueryService.class)
                            .getJobDetail(handle.id())
                            .orElseThrow()
                            .summary()
                            .status())
                    .isEqualTo(expected));
  }

  public static void noop() {}
}
