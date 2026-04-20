module run.ratchet.store.core {
  requires transitive run.ratchet.api;
  requires transitive jakarta.persistence;
  requires com.fasterxml.jackson.databind;
  requires com.fasterxml.jackson.datatype.jsr310;
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
  exports run.ratchet.store.util;

  opens run.ratchet.store.converter;
  opens run.ratchet.store.entity;
  opens run.ratchet.store.id;
}
