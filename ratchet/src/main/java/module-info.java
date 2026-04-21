module run.ratchet.ri {
  requires run.ratchet.api;
  requires run.ratchet.store.core;
  requires com.cronutils;
  requires com.fasterxml.jackson.databind;
  requires com.fasterxml.jackson.datatype.jsr310;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires jakarta.interceptor;
  requires jakarta.transaction;
  requires java.management;
  requires org.jboss.logging;
  requires org.objectweb.asm;
  requires org.objectweb.asm.tree;

  opens run.ratchet.ri.cdi;
  opens run.ratchet.ri.core;
  opens run.ratchet.ri.payload;
  opens run.ratchet.ri.resilience;
  opens run.ratchet.ri.security;
  opens run.ratchet.ri.util;
}
