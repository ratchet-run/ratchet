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
package run.ratchet.testsuite.infra;

import java.util.logging.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import run.ratchet.testsuite.util.PerformanceReportWriter;

/**
 * Client-side JUnit 5 extension that prints a consolidated performance summary table and writes the
 * combined JSON report after all performance tests have completed.
 *
 * <p>Each performance test class writes per-class fragment files via {@link
 * PerformanceReportWriter#writeClassFragment(String)}. This extension uses the {@link
 * ExtensionContext.Store.CloseableResource} pattern to aggregate all fragments into a single report
 * when the JVM-global test context shuts down.
 *
 * <p>Registered via {@code META-INF/services/org.junit.jupiter.api.extension.Extension}.
 */
public class PerformanceSummaryExtension
    implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

  private static final Logger log = Logger.getLogger(PerformanceSummaryExtension.class.getName());

  private static final String STORE_KEY = "ratchet-perf-summary";
  private static volatile boolean registered = false;

  @Override
  public void beforeAll(ExtensionContext context) {
    if (registered) {
      return;
    }

    synchronized (PerformanceSummaryExtension.class) {
      if (registered) {
        return;
      }

      context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL).put(STORE_KEY, this);
      registered = true;
      log.info("PerformanceSummaryExtension registered for end-of-suite aggregation");
    }
  }

  @Override
  public void close() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    log.info("Aggregating performance results for database: " + dbType);
    PerformanceReportWriter.writeAggregateReport(dbType);
  }
}
