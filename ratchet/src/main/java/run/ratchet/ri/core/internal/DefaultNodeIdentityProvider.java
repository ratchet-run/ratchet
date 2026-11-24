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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jboss.logging.Logger;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.internal.StringKeyNormalizer;

/**
 * Resolves the node ID: explicit ID, then hostname-PID-UUID, then a random UUID fallback. The
 * runtime maintains liveness independently through {@link NodeLiveness} for every provider.
 */
public class DefaultNodeIdentityProvider implements NodeIdentityProvider {
  private static final Logger log = Logger.getLogger(DefaultNodeIdentityProvider.class);
  private final ExecutorProvider executorProvider;
  private final String explicitNodeId;
  private volatile String nodeId;

  protected DefaultNodeIdentityProvider() {
    this(null, null);
  }

  public DefaultNodeIdentityProvider(ExecutorProvider executorProvider, String explicitNodeId) {
    this.executorProvider = executorProvider;
    this.explicitNodeId = StringKeyNormalizer.optional(explicitNodeId, "node id");
  }

  @Override
  public String getNodeId() {
    String current = nodeId;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      if (nodeId == null) {
        nodeId = resolveNodeId();
      }
      return nodeId;
    }
  }

  private String resolveNodeId() {
    if (explicitNodeId != null) {
      return explicitNodeId;
    }
    try {
      String host = resolveHostnameWithTimeout();
      String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
      return host + "-" + pid + "-" + UUID.randomUUID().toString().substring(0, 8);
    } catch (Exception e) {
      log.warnf(e, "Hostname resolution error, using UUID fallback: %s", e.getMessage());
      return UUID.randomUUID().toString();
    }
  }

  private String resolveHostnameWithTimeout() throws Exception {
    if (executorProvider == null || executorProvider.getJobExecutor() == null) {
      return InetAddress.getLocalHost().getHostName();
    }
    Future<String> future =
        executorProvider.getJobExecutor().submit(() -> InetAddress.getLocalHost().getHostName());
    try {
      return future.get(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new TimeoutException("DNS hostname lookup timed out after 5 seconds");
    }
  }
}
