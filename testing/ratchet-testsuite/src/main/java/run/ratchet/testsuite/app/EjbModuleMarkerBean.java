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
package run.ratchet.testsuite.app;

import jakarta.ejb.Singleton;

/**
 * Trivial {@code @Singleton} session bean whose only purpose is to make its jar a genuine EJB
 * module, mirroring the {@code nets-ejb} subdeployment. An ejb-jar with no session beans is an edge
 * case some containers handle differently, so this bean keeps the subdeployment realistic.
 */
@Singleton
public class EjbModuleMarkerBean {

  public void noop() {}
}
