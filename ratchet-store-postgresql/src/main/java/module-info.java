module run.ratchet.store.postgresql {
  requires run.ratchet.api;
  requires run.ratchet.store.core;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires jakarta.persistence;
  requires jakarta.transaction;
  requires java.sql;
  requires org.jboss.logging;

  exports run.ratchet.store.postgresql;

  opens run.ratchet.store.postgresql;
}
