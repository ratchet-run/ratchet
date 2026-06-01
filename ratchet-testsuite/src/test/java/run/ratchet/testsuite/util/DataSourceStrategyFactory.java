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
package run.ratchet.testsuite.util;

public final class DataSourceStrategyFactory {

  private DataSourceStrategyFactory() {}

  /** Reads {@code arquillian.launch} to pick the right strategy. */
  public static DataSourceStrategy create() {
    String launch = System.getProperty("arquillian.launch", "wildfly-managed");
    return switch (launch) {
      case "wildfly-managed", "wildfly-ee11-managed" -> new WildflyDataSourceStrategy();
      case "payara-managed" -> new PayaraDataSourceStrategy();
      case "glassfish-managed" -> new GlassFishDataSourceStrategy();
      case "openliberty-managed" -> new OpenLibertyDataSourceStrategy();
      default -> throw new IllegalArgumentException("No DataSourceStrategy for server: " + launch);
    };
  }
}
