package run.ratchet.testsuite.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/** Produces MongoDB test handles from MongoContainerExtension system properties. */
@ApplicationScoped
public class TestMongoProducer {

  private MongoClient client;

  @Produces
  @ApplicationScoped
  public MongoClient mongoClient() {
    String uri = TestRuntimeConfig.mongoUri();

    if (uri == null || uri.isBlank()) {
      throw new IllegalStateException(
          "ratchet.test.mongo.uri system property not set. "
              + "Ensure MongoContainerExtension is active and the mongodb profile is enabled.");
    }

    client = MongoClients.create(uri);
    return client;
  }

  @Produces
  @ApplicationScoped
  public MongoDatabase mongoDatabase(MongoClient mongoClient) {
    String dbName = TestRuntimeConfig.mongoDatabase();

    client = mongoClient;
    return client.getDatabase(dbName);
  }

  @PreDestroy
  void cleanup() {
    if (client != null) {
      client.close();
    }
  }
}
