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
module run.ratchet.tck.api {
  requires transitive run.ratchet.api;
  requires run.ratchet.tck.util;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires org.junit.jupiter.api;

  exports run.ratchet.tck.api;

  opens run.ratchet.tck.api;

  provides org.junit.platform.launcher.TestExecutionListener with
      run.ratchet.tck.api.ApiConformanceReportExtension;
}
