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
module run.ratchet.tck.store {
  requires run.ratchet.tck.util;
  requires run.ratchet.api;
  requires run.ratchet.store.core;
  requires jakarta.persistence;
  requires jakarta.transaction;
  requires java.sql;
  requires org.junit.jupiter.api;
  requires org.junit.platform.launcher;

  exports run.ratchet.tck.store;
  exports run.ratchet.tck.store.schema;

  provides org.junit.platform.launcher.TestExecutionListener with
      run.ratchet.tck.store.ConformanceReportExtension;
}
