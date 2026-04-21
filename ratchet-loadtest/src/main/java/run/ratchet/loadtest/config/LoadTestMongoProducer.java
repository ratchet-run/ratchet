package run.ratchet.loadtest.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class LoadTestMongoProducer {

  private MongoClient client;

  private static String envOrDefault(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  @Produces
  @ApplicationScoped
  public MongoDatabase mongoDatabase() {
    String uri = envOrDefault("MONGO_URI", "mongodb://mongo:27017");
    String database = envOrDefault("MONGO_DATABASE", "ratchet");
    client = MongoClients.create(uri);
    return client.getDatabase(database);
  }

  @PreDestroy
  void close() {
    if (client != null) {
      client.close();
    }
  }
}
