package run.ratchet.testsuite.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * CDI producer that provides a {@link MongoDatabase} bean inside WildFly for integration tests.
 *
 * <p>Reads the connection string and database name from system properties set by {@code
 * MongoContainerExtension} on the client side. These properties are forwarded to the WildFly JVM
 * via {@code arquillian.xml} JVM arguments.
 *
 * <p>This class is only deployed in the WAR when the {@code mongodb} profile is active. For JPA
 * store profiles, it is not included in the archive.
 */
@ApplicationScoped
public class TestMongoProducer {

  private MongoClient client;

  @Produces
  @ApplicationScoped
  public MongoDatabase mongoDatabase() {
    String uri = System.getProperty("ratchet.test.mongo.uri");
    String dbName = System.getProperty("ratchet.test.mongo.database", "ratchet_test");

    if (uri == null || uri.isBlank()) {
      throw new IllegalStateException(
          "ratchet.test.mongo.uri system property not set. "
              + "Ensure MongoContainerExtension is active and the mongodb profile is enabled.");
    }

    client = MongoClients.create(uri);
    return client.getDatabase(dbName);
  }

  @PreDestroy
  void cleanup() {
    if (client != null) {
      client.close();
    }
  }
}
