package run.ratchet.coordinator.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.concurrent.TimeUnit;

/**
 * Two-node embedded Hazelcast cluster for tests. Both members live in the same JVM and join via TCP
 * unicast loopback — no Docker, no multicast, deterministic discovery.
 */
public final class TwoNodeHazelcastCluster implements AutoCloseable {

  private final HazelcastInstance memberA;
  private final HazelcastInstance memberB;
  private final String clusterName;

  public TwoNodeHazelcastCluster() {
    this.clusterName = "ratchet-tck-" + Long.toHexString(System.nanoTime());
    // Spread the base port across a wide window to avoid collisions/TIME_WAIT between tests, and
    // keep auto-increment on so a bound port falls back to the next free one instead of failing
    // hard. The TcpIp member list covers the whole auto-increment window so discovery still works.
    int portA = 25700 + (int) (Long.remainderUnsigned(System.nanoTime(), PORT_RANGE)) * PORT_COUNT;
    int portB = portA + PORT_COUNT;
    this.memberA = Hazelcast.newHazelcastInstance(memberConfig("memberA", portA, portA, portB));
    this.memberB = Hazelcast.newHazelcastInstance(memberConfig("memberB", portB, portA, portB));
    awaitClusterFormed();
  }

  /** Number of ports auto-increment may walk through from each base port. */
  private static final int PORT_COUNT = 20;

  /** Number of distinct base-port pairs the cluster spreads across. */
  private static final int PORT_RANGE = 1000;

  public HazelcastInstance memberA() {
    return memberA;
  }

  public HazelcastInstance memberB() {
    return memberB;
  }

  /** Shut member A down — used to model a cluster-side disruption from B's perspective. */
  public void shutdownMemberA() {
    memberA.shutdown();
  }

  @Override
  public void close() {
    safeShutdown(memberA);
    safeShutdown(memberB);
  }

  private static void safeShutdown(HazelcastInstance h) {
    try {
      if (h != null) {
        h.shutdown();
      }
    } catch (Exception ignored) {
      // best-effort
    }
  }

  private Config memberConfig(String name, int port, int portA, int portB) {
    Config c = new Config();
    c.setInstanceName(name + "-" + Long.toHexString(System.nanoTime()));
    c.setClusterName(clusterName);
    NetworkConfig net = c.getNetworkConfig();
    net.setPort(port).setPortAutoIncrement(true).setPortCount(PORT_COUNT);
    JoinConfig join = net.getJoin();
    join.getMulticastConfig().setEnabled(false);
    join.getAutoDetectionConfig().setEnabled(false);
    TcpIpConfig tcp = join.getTcpIpConfig();
    tcp.setEnabled(true);
    // List every port the two members might auto-increment onto so discovery survives a fallback.
    for (int p = portA; p < portA + PORT_COUNT; p++) {
      tcp.addMember("127.0.0.1:" + p);
    }
    for (int p = portB; p < portB + PORT_COUNT; p++) {
      tcp.addMember("127.0.0.1:" + p);
    }
    // Silence the verbose Hazelcast banner & metrics noise on the test JVM.
    c.setProperty("hazelcast.logging.type", "none");
    c.setProperty("hazelcast.shutdownhook.enabled", "false");
    c.setProperty("hazelcast.phone.home.enabled", "false");
    return c;
  }

  private void awaitClusterFormed() {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (System.nanoTime() < deadline) {
      if (memberA.getCluster().getMembers().size() == 2
          && memberB.getCluster().getMembers().size() == 2) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted awaiting cluster formation", e);
      }
    }
    throw new IllegalStateException("Hazelcast cluster did not form 2 members within 15s");
  }
}
