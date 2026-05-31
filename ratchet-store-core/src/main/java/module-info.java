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
module run.ratchet.store.core {
  requires transitive run.ratchet.api;
  requires transitive jakarta.persistence;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires jakarta.interceptor;
  requires jakarta.json;
  requires jakarta.json.bind;
  requires java.sql;
  requires org.jboss.logging;
  requires org.objectweb.asm;

  exports run.ratchet.store;
  exports run.ratchet.store.converter;
  exports run.ratchet.store.dto;
  exports run.ratchet.store.entity;
  exports run.ratchet.store.id;
  exports run.ratchet.store.migration;
  exports run.ratchet.store.spi;
  exports run.ratchet.store.query;
  exports run.ratchet.store.util;

  opens run.ratchet.store.converter;
  opens run.ratchet.store.entity;
  opens run.ratchet.store.id;
  opens run.ratchet.store.migration;
}
