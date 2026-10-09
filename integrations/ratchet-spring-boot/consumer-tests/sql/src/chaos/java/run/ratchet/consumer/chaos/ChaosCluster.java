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
package run.ratchet.consumer.chaos;

import example.chaos.ChaosNodeApplication;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import run.ratchet.consumer.RuntimeProcess;
import run.ratchet.consumer.sql.SqlDatabase;

/** Independent Boot JVMs sharing one database and a database-clock fault timeline. */
public final class ChaosCluster implements AutoCloseable {
  private final SqlDatabase database;
  private final List<RuntimeProcess> nodes = new ArrayList<>();
  private final List<String> incarnations = new ArrayList<>();
  private final Set<Integer> stopped = ConcurrentHashMap.newKeySet();
  private final AtomicInteger next = new AtomicInteger();
  private final Set<String> submitted = ConcurrentHashMap.newKeySet();
  public final String run = UUID.randomUUID().toString();
  public final JdbcTemplate jdbc;
  public final DbClock clock;
  public final StoreProbe probe;
  public final FaultTimeline timeline = new FaultTimeline();

  public ChaosCluster() throws Exception {
    this(3);
  }

  public ChaosCluster(int count) throws Exception {
    String store = System.getProperty("store", "postgresql");
    DbClock.expression(store); // Reject unsupported stores before starting a container.
    database = SqlDatabase.start(store);
    var properties = database.properties();
    jdbc =
        new JdbcTemplate(
            new DriverManagerDataSource(
                (String) properties.get("spring.datasource.url"),
                (String) properties.get("spring.datasource.username"),
                (String) properties.get("spring.datasource.password")));
    clock = new DbClock(jdbc, store);
    probe = new StoreProbe(jdbc, store);
    try {
      for (int i = 0; i < count; i++) {
        nodes.add(null);
        incarnations.add(null);
        start(i);
      }
    } catch (Exception | AssertionError failure) {
      try {
        close();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private void start(int i) throws Exception {
    String incarnation = UUID.randomUUID().toString();
    var properties = new LinkedHashMap<>(database.properties());
    properties.put("chaos.store", database.store());
    properties.put("chaos.incarnation", incarnation);
    properties.put("ratchet.node.id", nodeId(i));
    properties.put("ratchet.allowed-packages", "example.ratchet,example.chaos");
    properties.put("ratchet.node.heartbeat-interval-seconds", 1);
    properties.put("ratchet.node.dynamic-heartbeat-enabled", false);
    properties.put("ratchet.node.orphan-grace-seconds", 3);
    properties.put("ratchet.node.orphan-scan-interval-seconds", 1);
    properties.put("ratchet.node.orphan-recovery-lease-ttl-seconds", 5);
    properties.put("ratchet.poller.min-delay-ms", 50);
    properties.put("ratchet.poller.max-delay-ms", 500);
    properties.put("ratchet.shutdown-timeout", "5s");
    properties.put("spring.flyway.enabled", false);
    properties.put("spring.liquibase.enabled", false);
    properties.put("spring.main.web-application-type", "servlet");
    properties.put("server.port", 0);
    properties.put("server.shutdown", "graceful");
    Files.createDirectories(Path.of("target"));
    RuntimeProcess process =
        new RuntimeProcess(
            ChaosNodeApplication.class, properties, nodeId(i) + "-" + incarnation.substring(0, 8));
    nodes.set(i, process);
    incarnations.set(i, incarnation);
    process.ready();
  }

  public RuntimeProcess node(int i) {
    return nodes.get(i);
  }

  public List<RuntimeProcess> nodes() {
    return List.copyOf(nodes);
  }

  public String nodeId(int i) {
    return "chaos-node-" + i;
  }

  public Set<String> submitted() {
    return Set.copyOf(submitted);
  }

  public String store() {
    return database.store();
  }

  public synchronized String submit(String key, long durationMs) throws Exception {
    for (int attempt = 0; attempt < nodes.size(); attempt++) {
      int i = Math.floorMod(next.getAndIncrement(), nodes.size());
      if (node(i).process.isAlive() && !stopped.contains(i))
        return submit(i, key, durationMs, "", "");
    }
    throw new IllegalStateException("No live node for submission");
  }

  public String submit(
      int i, String key, long durationMs, String idempotencyKey, String businessKey)
      throws Exception {
    String id =
        node(i)
            .post(
                "/submit?run="
                    + encode(run)
                    + "&key="
                    + encode(key)
                    + "&durationMs="
                    + durationMs
                    + "&idempotencyKey="
                    + encode(idempotencyKey)
                    + "&businessKey="
                    + encode(businessKey))
            .trim();
    submitted.add(id);
    return id;
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  public void kill(int i) throws Exception {
    fault(i, "kill");
  }

  public void terminate(int i) throws Exception {
    fault(i, "terminate");
  }

  public void stop(int i) throws Exception {
    fault(i, "stop");
  }

  private synchronized void fault(int i, String fault) throws Exception {
    long before = clock.now();
    switch (fault) {
      case "kill" -> node(i).kill();
      case "terminate" -> node(i).close();
      case "stop" -> {
        node(i).stop();
        stopped.add(i);
        awaitState(i, true);
      }
      default -> throw new IllegalArgumentException(fault);
    }
    timeline.add(
        new FaultTimeline.Entry(nodeId(i), incarnations.get(i), fault, before, clock.now(), null));
  }

  private void awaitState(int i, boolean suspended) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (System.nanoTime() < deadline) {
      String stat = Files.readString(Path.of("/proc", Long.toString(node(i).pid()), "stat"));
      boolean isStopped = stat.substring(stat.lastIndexOf(')') + 2).startsWith("T");
      if (isStopped == suspended) return;
      Thread.sleep(20);
    }
    throw new AssertionError("Process state not confirmed for " + nodeId(i));
  }

  public void resume(int i) throws Exception {
    node(i).resume();
    awaitState(i, false);
    stopped.remove(i);
    timeline.end(incarnations.get(i), clock.now());
  }

  public void restart(int i) throws Exception {
    if (node(i).process.isAlive())
      throw new IllegalStateException("Kill or terminate before restarting " + nodeId(i));
    String old = incarnations.get(i);
    start(i);
    stopped.remove(i);
    timeline.end(old, clock.now());
  }

  private Set<String> live() {
    var result = ConcurrentHashMap.<String>newKeySet();
    for (int i = 0; i < nodes.size(); i++)
      if (node(i).process.isAlive() && !stopped.contains(i)) result.add(nodeId(i));
    return result;
  }

  public Ledger ledger() {
    return Ledger.load(jdbc, run, store());
  }

  public List<Oracle.Violation> settle(Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      var statuses = probe.statuses(run);
      if (submitted.stream().allMatch(id -> Oracle.terminal(statuses.get(id)))
          && probe.quiescence(live()).isEmpty()) break;
      Thread.sleep(100);
    }
    return Oracle.evaluate(
        submitted(), probe.statuses(run), ledger(), timeline, probe.quiescence(live()));
  }

  public int busiest(int minimum, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      var counts =
          ledger().attempts().stream()
              .filter(a -> a.finishedUs() == null)
              .collect(Collectors.groupingBy(Ledger.Attempt::nodeId, Collectors.counting()));
      int best = 0;
      for (int i = 1; i < nodes.size(); i++)
        if (counts.getOrDefault(nodeId(i), 0L) > counts.getOrDefault(nodeId(best), 0L)) best = i;
      if (counts.getOrDefault(nodeId(best), 0L) >= minimum) return best;
      Thread.sleep(10);
    }
    throw new AssertionError("No node had " + minimum + " open attempts before the fault deadline");
  }

  @Override
  public void close() throws Exception {
    Exception failure = null;
    for (int i = 0; i < nodes.size(); i++) {
      if (nodes.get(i) == null) continue;
      try {
        if (stopped.contains(i) && node(i).process.isAlive()) node(i).resume();
        node(i).close();
      } catch (Exception cleanup) {
        if (failure == null) failure = cleanup;
        else failure.addSuppressed(cleanup);
      }
    }
    try {
      database.close();
    } catch (Exception cleanup) {
      if (failure == null) failure = cleanup;
      else failure.addSuppressed(cleanup);
    }
    if (failure != null) throw failure;
  }
}
