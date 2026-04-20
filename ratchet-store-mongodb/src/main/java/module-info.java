module run.ratchet.store.mongodb {
  requires run.ratchet.api;
  requires run.ratchet.store.core;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires org.jboss.logging;
  requires org.mongodb.bson;
  requires org.mongodb.driver.core;
  requires org.mongodb.driver.sync.client;

  exports run.ratchet.store.mongodb;

  opens run.ratchet.store.mongodb;
}
