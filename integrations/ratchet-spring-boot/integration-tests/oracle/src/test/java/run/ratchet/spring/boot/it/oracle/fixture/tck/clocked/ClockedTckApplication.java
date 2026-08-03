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
package run.ratchet.spring.boot.it.oracle.fixture.tck.clocked;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/** Isolated Spring fixture for deterministic delayed-scheduling coverage. */
@SpringBootApplication(
    scanBasePackages = "run.ratchet.spring.boot.it.oracle.fixture.tck.clocked.consumer",
    excludeName = "run.ratchet.spring.boot.autoconfigure.jpa.RatchetJpaAutoConfiguration")
@Import(ClockedTckConfiguration.class)
public class ClockedTckApplication {}
