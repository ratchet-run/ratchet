module run.ratchet.store.mysql {
  requires run.ratchet.api;
  requires run.ratchet.store.core;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires jakarta.persistence;
  requires jakarta.transaction;
  requires java.sql;
  requires org.jboss.logging;

  exports run.ratchet.store.mysql;

  opens run.ratchet.store.mysql;
}
