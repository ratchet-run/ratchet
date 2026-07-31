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
package run.ratchet.ri.core.internal;

import java.util.List;
import java.util.Objects;
import org.jboss.logging.Logger;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.PayloadSerializerHolder;

/** Installs the unambiguous container-managed payload serializer for store converters. */
public final class PayloadSerializerRuntimeInstallation implements RuntimeInstallation {

  // Keep the pre-extraction logger category for stable operational filtering.
  private static final Logger log = Logger.getLogger("run.ratchet.ri.cdi.RatchetProducer");

  private final PayloadSerializer serializer;

  public PayloadSerializerRuntimeInstallation(List<PayloadSerializer> serializers) {
    List<PayloadSerializer> candidates =
        List.copyOf(Objects.requireNonNull(serializers, "serializers"));
    serializer = candidates.size() == 1 ? candidates.get(0) : null;
  }

  @Override
  public void install(Object ownerToken) {
    if (serializer == null) {
      log.warn(
          "No PayloadSerializer bean resolvable at startup; JPA converters will use fallback"
              + " JSON-B.");
    }
    PayloadSerializerHolder.install(ownerToken, serializer);
  }

  @Override
  public void uninstall(Object ownerToken) {
    PayloadSerializerHolder.uninstall(ownerToken);
  }
}
