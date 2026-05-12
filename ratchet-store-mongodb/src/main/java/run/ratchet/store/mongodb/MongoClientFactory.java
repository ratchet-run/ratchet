package run.ratchet.store.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.Objects;
import org.bson.UuidRepresentation;

/**
 * Library-owned construction of {@link MongoClient} for ratchet-store-mongodb.
 *
 * <p>Forces {@link UuidRepresentation#STANDARD} on the codec registry. Mongo's historical default
 * {@code JAVA_LEGACY} silently swaps UUID byte order, which corrupts the timestamp prefix on RFC
 * 9562 §5.7 UUIDv7 round-trips and breaks time-correlation guarantees.
 *
 * <p>When a host application supplies its own {@code MongoClient}, validation is performed at
 * startup by the {@code @PostConstruct} guard on {@code MongoJobStoreImpl}, which throws {@link
 * run.ratchet.store.RatchetConfigurationException} on a non-STANDARD codec.
 */
public final class MongoClientFactory {

  private MongoClientFactory() {}

  /**
   * Create a {@link MongoClient} with Ratchet's required UUID settings.
   *
   * <p>The caller owns the returned client and must close it, usually from {@code @PreDestroy}, to
   * stop MongoDB connection-pool threads during undeploy or test teardown.
   */
  public static MongoClient create(String connectionString) {
    Objects.requireNonNull(connectionString, "connectionString");
    if (connectionString.isBlank()) {
      throw new IllegalArgumentException("connectionString must not be blank");
    }
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(connectionString))
            .uuidRepresentation(UuidRepresentation.STANDARD)
            .build();
    return MongoClients.create(settings);
  }
}
