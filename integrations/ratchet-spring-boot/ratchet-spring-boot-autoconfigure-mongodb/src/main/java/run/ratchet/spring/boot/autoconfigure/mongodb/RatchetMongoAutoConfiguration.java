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
package run.ratchet.spring.boot.autoconfigure.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.mongodb.MongoClientFactory;
import run.ratchet.store.mongodb.MongoJobStore;
import run.ratchet.store.mongodb.MongoJobStoreFactory;
import run.ratchet.store.spi.JobStore;

@AutoConfiguration(
    beforeName = {
      "org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration",
      "org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration"
    })
@ConditionalOnClass(MongoJobStore.class)
@ConditionalOnProperty(name = "ratchet.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RatchetMongoProperties.class)
public class RatchetMongoAutoConfiguration {

  /** Isolates optional MongoDB store types until the outer classpath condition has matched. */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(MongoJobStore.class)
  static class MongoStoreConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(MongoClient.class)
    @ConditionalOnProperty(name = RatchetMongoProperties.CONNECTION_STRING_PROPERTY)
    MongoClient ratchetMongoClient(RatchetMongoProperties properties) {
      return MongoClientFactory.create(properties.getConnectionString());
    }

    @Bean
    @ConditionalOnMissingBean(MongoDatabase.class)
    @ConditionalOnProperty(name = RatchetMongoProperties.CONNECTION_STRING_PROPERTY)
    MongoDatabase ratchetMongoDatabase(MongoClient mongoClient, RatchetMongoProperties properties) {
      String database = properties.getDatabase();
      if (database == null || database.isBlank()) {
        throw new IllegalStateException(
            RatchetMongoProperties.DATABASE_PROPERTY
                + " must be set when "
                + RatchetMongoProperties.CONNECTION_STRING_PROPERTY
                + " is configured");
      }
      return mongoClient.getDatabase(database);
    }

    @Bean
    @ConditionalOnMissingBean(JobStore.class)
    @ConditionalOnProperty(name = RatchetMongoProperties.CONNECTION_STRING_PROPERTY)
    MongoJobStore ratchetMongoJobStore(
        MongoClient mongoClient,
        MongoDatabase mongoDatabase,
        RatchetOptions options,
        MetricsCollector metricsCollector) {
      return MongoJobStoreFactory.create(mongoClient, mongoDatabase, options, metricsCollector);
    }
  }
}
