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
package run.ratchet.spring.boot.it.mongodb.fixture.tck;

import java.util.UUID;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import run.ratchet.store.mongodb.MongoSharedContainer;

/** Supplies the shared MongoDB container before Spring creates the application context. */
public final class MongoTckApplicationContextInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  private static final String DATABASE_NAME =
      "ratchet_spring_tck_" + UUID.randomUUID().toString().replace("-", "");

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    TestPropertyValues.of(
            "ratchet.mongodb.connection-string=" + MongoSharedContainer.connectionString(),
            "ratchet.mongodb.database=" + DATABASE_NAME,
            "ratchet.class-policy.allowed-packages="
                + MongoTckConfiguration.APPLICATION_PACKAGE
                + ","
                + MongoTckConfiguration.TCK_PACKAGE,
            "ratchet.lifecycle.drain-timeout=PT30S")
        .applyTo(applicationContext.getEnvironment());
  }
}
