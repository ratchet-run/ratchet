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
package run.ratchet.showcase;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class DashboardConfigurationTest {

  @Test
  void paymentOutageButtonUsesDemoSafeRetryWindow() throws IOException {
    String app = Files.readString(Path.of("src/main/webapp/app.js"));

    assertTrue(app.contains("const PAYMENT_OUTAGE_SECONDS = 5;"));
    assertTrue(app.contains("seconds: PAYMENT_OUTAGE_SECONDS"));
    assertFalse(app.contains("seconds: 45"));
  }

  @Test
  void dashboardButtonsCannotOverlapAsyncActions() throws IOException {
    String app = Files.readString(Path.of("src/main/webapp/app.js"));

    assertTrue(app.contains("let actionInFlight = false;"));
    assertTrue(app.contains("setActionsBusy(true);"));
    assertTrue(app.contains("setActionsBusy(false);"));
    assertTrue(app.contains("!button || actionInFlight"));
  }

  @Test
  void rateSliderAllowsThousandOrdersPerMinute() throws IOException {
    String html = Files.readString(Path.of("src/main/webapp/index.html"));

    assertTrue(html.contains("id=\"rate\" type=\"range\" min=\"1\" max=\"1000\""));
    assertFalse(html.contains("id=\"rate\" type=\"range\" min=\"1\" max=\"240\""));
  }

  @Test
  void dashboardSurfacesRatchetFeaturePanels() throws IOException {
    String html = Files.readString(Path.of("src/main/webapp/index.html"));
    String app = Files.readString(Path.of("src/main/webapp/app.js"));

    assertTrue(html.contains("id=\"activity\""));
    assertTrue(html.contains("id=\"jobDetail\""));
    assertTrue(html.contains("id=\"metricsPreview\""));
    assertTrue(html.contains("id=\"metricsDetail\""));
    assertFalse(html.contains("href=\"../metrics\""));
    assertTrue(html.contains("id=\"fraudReview\""));
    assertTrue(html.contains("id=\"badCard\""));
    assertTrue(html.contains("id=\"warehouseCrunch\""));
    assertTrue(html.contains("id=\"carrierOutage\""));
    assertTrue(app.contains("renderActivity(data)"));
    assertTrue(app.contains("data-detail-job"));
    assertTrue(app.contains("fetch(\"metrics\""));
    assertTrue(app.contains("showMetricsDetail()"));
    assertTrue(app.contains("api/scenarios/fraud-review"));
    assertTrue(app.contains("api/scenarios/bad-card"));
    assertTrue(app.contains("api/scenarios/warehouse-crunch"));
    assertTrue(app.contains("api/scenarios/carrier-outage"));
  }

  @Test
  void metricsModalRefreshesWhileOpenAndParsesLabeledSamples() throws IOException {
    String app = Files.readString(Path.of("src/main/webapp/app.js"));

    assertTrue(app.contains("const METRICS_REFRESH_MS = 1500;"));
    assertTrue(app.contains("let metricsRefreshTimer = null;"));
    assertTrue(app.contains("const refreshMetricsDetail = async () =>"));
    assertTrue(app.contains("startMetricsRefresh();"));
    assertTrue(app.contains("setInterval(() =>"));
    assertTrue(app.contains("stopMetricsRefresh();"));
    assertTrue(app.contains("addEventListener(\"close\", stopMetricsRefresh)"));
    assertTrue(
        app.contains(
            ".filter((line) => line.startsWith(`${name} `) || line.startsWith(`${name}{`))"));
    assertTrue(app.contains(".map((line) => Number(line.trim().split(/\\s+/).at(-1)))"));
    assertFalse(app.contains("new RegExp(`^${name}"));
  }
}
