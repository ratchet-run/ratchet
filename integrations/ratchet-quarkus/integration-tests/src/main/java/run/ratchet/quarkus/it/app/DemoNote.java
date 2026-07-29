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
package run.ratchet.quarkus.it.app;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

/**
 * An application-owned entity mapped to the <em>default</em> persistence unit — not Ratchet's. Its
 * presence proves the named "ratchet" unit and the app's own unit coexist: the app keeps its
 * entities and its own schema-generation policy while Ratchet runs on an isolated unit.
 */
@Entity
public class DemoNote {

  @Id public Long id;

  public String text;
}
