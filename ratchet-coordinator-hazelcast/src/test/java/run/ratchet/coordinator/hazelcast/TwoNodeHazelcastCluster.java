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
    int portA = 25700 + (int) (System.nanoTime() & 0x7F);
    int portB = portA + 1;
    this.memberA = Hazelcast.newHazelcastInstance(memberConfig("memberA", portA, portA, portB));
    this.memberB = Hazelcast.newHazelcastInstance(memberConfig("memberB", portB, portA, portB));
    awaitClusterFormed();
  }

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
    net.setPort(port).setPortAutoIncrement(false);
    JoinConfig join = net.getJoin();
    join.getMulticastConfig().setEnabled(false);
    join.getAutoDetectionConfig().setEnabled(false);
    TcpIpConfig tcp = join.getTcpIpConfig();
    tcp.setEnabled(true);
    tcp.addMember("127.0.0.1:" + portA).addMember("127.0.0.1:" + portB);
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
