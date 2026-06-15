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
package run.ratchet.loadtest.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Publishes WildFly (IronJacamar) connection-pool statistics for the {@code RatchetDS} datasource
 * as Micrometer gauges, so the Prometheus scrape on {@code :8080/metrics} carries client-side pool
 * pressure alongside the existing store and JVM metrics.
 *
 * <p>This is the load-test triage instrument that separates <em>connection-pool wait</em> (an app
 * server phenomenon: threads blocked in {@code getConnection()}) from <em>database latency</em> (a
 * server phenomenon already visible via {@code mysqld-exporter}). The pool MBean only carries live
 * numbers when the datasource is defined with {@code statistics-enabled=true}; see the store CLI
 * files under {@code infra/loadtest/wildfly}.
 *
 * <p>Every numeric attribute the pool MBean exposes is published as {@code
 * ratchet.ds.pool.<attribute>} rather than hardcoding a known set, which keeps the binder robust
 * across WildFly versions and surfaces the full statistic set for offline analysis. Backends
 * without a JDBC datasource (MongoDB) have no such MBean; the binder degrades to a no-op.
 */
@ApplicationScoped
public class DataSourcePoolMetricsBinder {

  private static final String POOL_MBEAN =
      "jboss.as:subsystem=datasources,data-source=RatchetDS,statistics=pool";
  private static final Set<String> NUMERIC_TYPES =
      Set.of(
          "int",
          "long",
          "short",
          "byte",
          "double",
          "float",
          "java.lang.Integer",
          "java.lang.Long",
          "java.lang.Short",
          "java.lang.Byte",
          "java.lang.Double",
          "java.lang.Float",
          "java.math.BigInteger",
          "java.math.BigDecimal");

  @Inject MeterRegistry registry;

  private volatile boolean bound;

  /**
   * Registers the pool gauges once the MBean is available. Safe to call on every scrape: it retries
   * until the datasource pool MBean exists, then binds exactly once.
   */
  public void ensureBound() {
    if (bound) {
      return;
    }
    synchronized (this) {
      if (bound) {
        return;
      }
      bound = tryBind();
    }
  }

  private boolean tryBind() {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName poolName;
    try {
      poolName = new ObjectName(POOL_MBEAN);
    } catch (Exception e) {
      return true; // malformed name is permanent; do not retry every scrape
    }
    if (!server.isRegistered(poolName)) {
      // Either the datasource is not up yet (retry next scrape) or this is a non-JDBC backend.
      return false;
    }

    MBeanAttributeInfo[] attributes;
    try {
      attributes = server.getMBeanInfo(poolName).getAttributes();
    } catch (Exception e) {
      return false;
    }

    for (MBeanAttributeInfo attribute : attributes) {
      if (!attribute.isReadable() || !NUMERIC_TYPES.contains(attribute.getType())) {
        continue;
      }
      String attributeName = attribute.getName();
      Gauge.builder(
              "ratchet.ds.pool." + toSnakeCase(attributeName),
              () -> readNumeric(server, poolName, attributeName))
          .tag("datasource", "RatchetDS")
          .description(
              attribute.getDescription() == null ? attributeName : attribute.getDescription())
          .register(registry);
    }
    return true;
  }

  private static Number readNumeric(MBeanServer server, ObjectName poolName, String attributeName) {
    try {
      Object value = server.getAttribute(poolName, attributeName);
      return value instanceof Number number ? number : Double.NaN;
    } catch (Exception e) {
      return Double.NaN;
    }
  }

  private static String toSnakeCase(String camel) {
    StringBuilder out = new StringBuilder(camel.length() + 8);
    for (int i = 0; i < camel.length(); i++) {
      char c = camel.charAt(i);
      if (Character.isUpperCase(c)) {
        if (i > 0) {
          out.append('_');
        }
        out.append(Character.toLowerCase(c));
      } else {
        out.append(c);
      }
    }
    return out.toString();
  }
}
