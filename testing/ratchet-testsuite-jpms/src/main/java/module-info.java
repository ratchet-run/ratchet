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
/**
 * JPMS consumer test for Ratchet module descriptors.
 *
 * <p>This module exists solely to assert — at compile time — that {@code ratchet-api}'s {@code
 * module-info.java} correctly exports the public API/SPI packages for module-path consumers.
 *
 * <p>The single source file {@code test.jpms.consumer.JpmsConsumerProbe} imports types from the
 * exported {@code run.ratchet.api}, {@code run.ratchet.api.event}, {@code
 * run.ratchet.api.exception}, and {@code run.ratchet.spi} packages. Compile success of this module
 * IS the verification. The module also requires {@code run.ratchet.ri} to prove the RI resolves as
 * a named module without exporting its implementation packages.
 */
module ratchet.testsuite.jpms {
  requires run.ratchet.api;
  requires run.ratchet.ri;
  requires run.ratchet.coordinator.common;
  requires run.ratchet.coordinator.postgresql;
  requires run.ratchet.coordinator.jms;
  requires run.ratchet.coordinator.infinispan;
  requires run.ratchet.coordinator.hazelcast;
}
