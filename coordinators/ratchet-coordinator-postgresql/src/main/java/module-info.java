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
module run.ratchet.coordinator.postgresql {
  requires run.ratchet.api;
  requires run.ratchet.coordinator.common;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires jakarta.interceptor;
  requires java.sql;
  requires org.jboss.logging;
  requires org.postgresql.jdbc;

  exports run.ratchet.coordinator.postgresql;

  opens run.ratchet.coordinator.postgresql;
}
