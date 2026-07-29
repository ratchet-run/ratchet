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
package run.ratchet.quarkus.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.IsLocalDevelopment;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.devui.spi.JsonRPCProvidersBuildItem;
import io.quarkus.devui.spi.page.CardPageBuildItem;
import io.quarkus.devui.spi.page.Page;
import run.ratchet.quarkus.runtime.devui.RatchetDevUiJsonRPCService;

class RatchetDevUiProcessor {

  @BuildStep(onlyIf = IsLocalDevelopment.class)
  void pages(BuildProducer<CardPageBuildItem> pages) {
    CardPageBuildItem card = new CardPageBuildItem();
    card.addPage(
        Page.webComponentPageBuilder()
            .title("Jobs")
            .icon("font-awesome-solid:list-check")
            .componentLink("qwc-ratchet-jobs.js"));
    card.addPage(
        Page.webComponentPageBuilder()
            .title("Cluster")
            .icon("font-awesome-solid:server")
            .componentLink("qwc-ratchet-cluster.js"));
    pages.produce(card);
  }

  @BuildStep(onlyIf = IsLocalDevelopment.class)
  JsonRPCProvidersBuildItem rpc() {
    return new JsonRPCProvidersBuildItem(RatchetDevUiJsonRPCService.class);
  }

  @BuildStep(onlyIf = IsLocalDevelopment.class)
  AdditionalBeanBuildItem devUiJsonRpcService() {
    return AdditionalBeanBuildItem.builder()
        .addBeanClass(RatchetDevUiJsonRPCService.class)
        .setUnremovable()
        .build();
  }
}
