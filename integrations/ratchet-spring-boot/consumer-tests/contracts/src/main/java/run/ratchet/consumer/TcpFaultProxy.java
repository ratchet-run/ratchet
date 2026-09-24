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
package run.ratchet.consumer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** A real TCP interruption point; reconnects are rejected until the test restores connectivity. */
public final class TcpFaultProxy implements AutoCloseable {
  private final ServerSocket listener;
  private final String host;
  private final int upstreamPort;
  private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
  private final ExecutorService threads =
      Executors.newCachedThreadPool(
          r -> {
            var thread = new Thread(r, "consumer-wire-proxy");
            thread.setDaemon(true);
            return thread;
          });
  private volatile boolean available = true;
  private volatile boolean closed;
  public final AtomicInteger rejected = new AtomicInteger();

  public TcpFaultProxy(String host, int port) throws IOException {
    this.host = host;
    this.upstreamPort = port;
    listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    threads.submit(
        () -> {
          while (!closed) {
            Socket client = null;
            Socket upstream = null;
            try {
              client = listener.accept();
              if (!available) {
                rejected.incrementAndGet();
                client.close();
                continue;
              }
              upstream = new Socket();
              upstream.connect(new InetSocketAddress(this.host, upstreamPort), 2000);
              sockets.add(client);
              sockets.add(upstream);
              if (!available || closed) {
                close(client);
                close(upstream);
                continue;
              }
              Socket downstream = client;
              Socket connected = upstream;
              threads.submit(() -> copy(downstream, connected));
              threads.submit(() -> copy(connected, downstream));
            } catch (IOException failure) {
              if (client != null) close(client);
              if (upstream != null) close(upstream);
              if (!closed) rejected.incrementAndGet();
            }
          }
        });
  }

  public int port() {
    return listener.getLocalPort();
  }

  public void disconnect() {
    available = false;
    sockets.forEach(this::close);
  }

  public void reconnect() {
    available = true;
  }

  private void copy(Socket source, Socket target) {
    try {
      source.getInputStream().transferTo(target.getOutputStream());
    } catch (IOException ignored) {
      /* expected at an injected disconnect */
    } finally {
      close(source);
      close(target);
    }
  }

  private void close(Socket socket) {
    sockets.remove(socket);
    try {
      socket.close();
    } catch (IOException ignored) {
    }
  }

  @Override
  public void close() throws Exception {
    closed = true;
    listener.close();
    sockets.forEach(this::close);
    threads.shutdownNow();
    if (!threads.awaitTermination(5, TimeUnit.SECONDS))
      throw new AssertionError("Wire proxy threads leaked");
  }
}
