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
package run.ratchet.spring.boot.autoconfigure;

import java.util.Objects;
import java.util.function.Consumer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationEventPublisher;

/** Synchronous bridge from Ratchet's internal publisher to Spring application events. */
final class SpringEventBridge implements Consumer<Object>, DisposableBean {

  private volatile ApplicationEventPublisher publisher;

  SpringEventBridge(ApplicationEventPublisher publisher) {
    this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
  }

  @Override
  public void accept(Object event) {
    ApplicationEventPublisher currentPublisher = publisher;
    if (currentPublisher != null) {
      currentPublisher.publishEvent(event);
    }
  }

  @Override
  public void destroy() {
    publisher = null;
  }

  boolean hasPublisher() {
    return publisher != null;
  }
}
