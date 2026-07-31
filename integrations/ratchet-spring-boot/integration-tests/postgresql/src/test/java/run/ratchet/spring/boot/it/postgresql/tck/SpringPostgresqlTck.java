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
package run.ratchet.spring.boot.it.postgresql.tck;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import run.ratchet.spring.boot.it.postgresql.PostgresqlContainerExtension;
import run.ratchet.spring.boot.it.postgresql.fixture.tck.TckApplication;
import run.ratchet.spring.boot.it.postgresql.fixture.tck.TckApplicationContextInitializer;

/** Shared real-PostgreSQL Spring test context for API TCK bindings. */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(PostgresqlContainerExtension.class)
@SpringBootTest(classes = TckApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(initializers = TckApplicationContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@interface SpringPostgresqlTck {}
