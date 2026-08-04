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
package run.ratchet.spring.boot.it.sharedtck.tck;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.StoreContainerExtension;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.clocked.ClockedTckApplication;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.clocked.ClockedTckApplicationContextInitializer;

/** Separate context using the shared clocked in-memory store. */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(StoreContainerExtension.class)
@SpringBootTest(
    classes = ClockedTckApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(initializers = ClockedTckApplicationContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@interface SpringClockedTck {}
