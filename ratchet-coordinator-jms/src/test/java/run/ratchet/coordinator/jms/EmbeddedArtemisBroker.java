package run.ratchet.coordinator.jms;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

/**
 * In-JVM Artemis broker for JMS coordinator integration tests. Persistence and security are
 * disabled — the broker is purely a wakeup transport for the duration of the test class. The {@code
 * vm://} in-VM acceptor avoids picking a real TCP port and the inevitable port conflicts in CI.
 *
 * <p>Lifecycle is start/stop/start friendly: {@link #stop()} fully tears the broker down (the
 * coordinator under test will see {@code ExceptionListener} firings and start its reconnect loop);
 * {@link #start()} brings a fresh instance up on the same vm:// URL.
 */
public final class EmbeddedArtemisBroker {

  private final String connectorUrl;
  private final Path dataDir;
  private EmbeddedActiveMQ broker;

  public EmbeddedArtemisBroker() {
    // Use a stable, instance-scoped vm:// id so the connection factory URL stays valid across
    // stop+start cycles in a single test class.
    this.connectorUrl = "vm://" + System.nanoTime();
    try {
      this.dataDir = Files.createTempDirectory("ratchet-jms-tck");
    } catch (Exception e) {
      throw new IllegalStateException("could not allocate Artemis data directory", e);
    }
  }

  public String connectorUrl() {
    return connectorUrl;
  }

  public void start() throws Exception {
    if (broker != null) {
      return;
    }
    Configuration config =
        new ConfigurationImpl()
            .setName("ratchet-jms-tck-" + System.nanoTime())
            .setBindingsDirectory(dataDir.resolve("bindings").toString())
            .setJournalDirectory(dataDir.resolve("journal").toString())
            .setLargeMessagesDirectory(dataDir.resolve("large").toString())
            .setPagingDirectory(dataDir.resolve("paging").toString())
            .setPersistenceEnabled(false)
            .setSecurityEnabled(false)
            .setJMXManagementEnabled(false)
            .addAcceptorConfiguration("invm", connectorUrl);
    EmbeddedActiveMQ b = new EmbeddedActiveMQ();
    b.setConfiguration(config);
    b.start();
    this.broker = b;
  }

  public void stop() throws Exception {
    if (broker == null) {
      return;
    }
    try {
      broker.stop();
    } finally {
      broker = null;
    }
  }
}
