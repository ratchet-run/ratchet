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
package run.ratchet.spring.boot.autoconfigure.jpa;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.DisposableBean;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.PayloadSerializerHolder;

/** Owns the Spring runtime's installation in the JPA converter serializer holder. */
final class SpringPayloadSerializerInstallation implements DisposableBean {

  private final Object ownerToken = new Object();
  private final AtomicBoolean installed = new AtomicBoolean();

  SpringPayloadSerializerInstallation(PayloadSerializer payloadSerializer) {
    PayloadSerializerHolder.install(
        ownerToken, Objects.requireNonNull(payloadSerializer, "payloadSerializer"));
    installed.set(true);
  }

  @Override
  public void destroy() {
    if (installed.compareAndSet(true, false)) {
      PayloadSerializerHolder.uninstall(ownerToken);
    }
  }
}
