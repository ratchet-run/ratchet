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

import com.mongodb.ClientSessionOptions;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.mongodb.MongoJobStoreFactory;
import run.ratchet.store.spi.JobStore;

/** Spring Boot MongoDB store wiring for Ratchet. */
@AutoConfiguration(
    afterName = {
      "run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration",
      "org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration",
      "org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration",
      "org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration",
      "org.springframework.boot.data.mongodb.autoconfigure.MongoDataAutoConfiguration"
    },
    beforeName = "run.ratchet.spring.boot.autoconfigure.RatchetEngineAutoConfiguration")
@ConditionalOnClass(MongoDatabaseFactory.class)
@ConditionalOnBean(MongoDatabaseFactory.class)
@ConditionalOnMissingBean(JobStore.class)
@ConditionalOnProperty(
    prefix = "ratchet",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class RatchetMongoAutoConfiguration {

  @Bean
  @DependsOn("ratchetRuntimeInstallation")
  JobStore ratchetMongoJobStore(
      MongoDatabaseFactory databaseFactory,
      RatchetOptions options,
      MetricsCollector metricsCollector) {
    return MongoJobStoreFactory.create(
        databaseFactory.getMongoDatabase(),
        () -> databaseFactory.getSession(ClientSessionOptions.builder().build()),
        options,
        metricsCollector,
        options.schema().autoMigrate());
  }
}
