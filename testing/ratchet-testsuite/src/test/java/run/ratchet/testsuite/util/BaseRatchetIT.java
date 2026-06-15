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
package run.ratchet.testsuite.util;

import jakarta.inject.Inject;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.testsuite.app.TestCleanupStrategy;

/**
 * Base class for all Ratchet integration tests. Injects TestCleanupStrategy and truncates tables
 * before and after each test.
 */
@ExtendWith(ArquillianExtension.class)
public abstract class BaseRatchetIT {

  @Inject private TestCleanupStrategy cleanupStrategy;

  @BeforeEach
  protected void truncateAll() throws Exception {
    cleanupStrategy.truncateAll();
  }

  @AfterEach
  protected void cleanupAfterEach() throws Exception {
    cleanupStrategy.truncateAll();
  }
}
