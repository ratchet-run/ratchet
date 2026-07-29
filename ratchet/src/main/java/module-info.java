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
module run.ratchet.ri {
  requires run.ratchet.api;
  requires run.ratchet.store.core;
  requires run.ratchet.encryption;
  requires run.ratchet.security.jakarta;
  requires com.cronutils;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires jakarta.interceptor;
  requires jakarta.json.bind;
  requires jakarta.transaction;
  requires java.management;
  requires java.naming;
  requires org.jboss.logging;
  requires org.objectweb.asm;
  requires org.objectweb.asm.tree;

  provides jakarta.enterprise.inject.spi.Extension with
      run.ratchet.ri.cdi.RecurringMethodDiscoveryExtension;

  opens run.ratchet.ri.cdi;
  opens run.ratchet.ri.cdi.internal;
  opens run.ratchet.ri.core;
  opens run.ratchet.ri.core.internal;
  opens run.ratchet.ri.payload;
  opens run.ratchet.ri.resilience;
  opens run.ratchet.ri.security;
  opens run.ratchet.ri.util;
}
