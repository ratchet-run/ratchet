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
package example.ratchet;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import run.ratchet.api.JobQueryService;
import run.ratchet.consumer.sql.SqlDatabase;

class CallerPrincipalBridgeTest {
  private static SqlDatabase database;

  @BeforeAll
  static void startDatabase() {
    database = SqlDatabase.start();
  }

  @AfterAll
  static void stopDatabase() {
    if (database != null) database.close();
  }

  @AfterEach
  void clearContext() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void authenticatedSubmitterIsRecorded() {
    assertCallerPrincipal(
        UsernamePasswordAuthenticationToken.authenticated("consumer-alice", "", List.of()),
        "consumer-alice");
  }

  @Test
  void anonymousSubmitterRecordsNull() {
    assertCallerPrincipal(
        new AnonymousAuthenticationToken(
            "key", "anonymousUser", List.of(new SimpleGrantedAuthority("ROLE_ANONYMOUS"))),
        null);
  }

  @Test
  void emptyContextRecordsNull() {
    assertCallerPrincipal(null, null);
  }

  private void assertCallerPrincipal(Authentication authentication, String expected) {
    try (var context =
        new SpringApplicationBuilder(ConsumerApplication.class)
            .properties(database.properties())
            .run()) {
      SecurityContextHolder.clearContext();
      SecurityContextHolder.getContext().setAuthentication(authentication);
      var handle = context.getBean(ConsumerService.class).submit("principal-" + UUID.randomUUID());
      assertThat(
              context
                  .getBean(JobQueryService.class)
                  .getJobDetail(handle.id())
                  .orElseThrow()
                  .summary()
                  .callerPrincipal())
          .isEqualTo(expected);
    }
  }
}
