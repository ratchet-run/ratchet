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
package example.ratchet.mongo;

import java.time.Duration;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;

@SpringBootApplication
public class MongoConsumerApplication {

  public static void main(String[] args) {
    var context = SpringApplication.run(MongoConsumerApplication.class, args);
    if (context.getEnvironment().getProperty("consumer.verify", Boolean.class, false)) {
      context.close();
    }
  }

  @Bean
  @ConditionalOnProperty(name = "consumer.verify", havingValue = "true")
  ApplicationRunner verifyConsumer(
      MongoConsumerService service, MongoTemplate mongoTemplate, Environment environment) {
    return args -> {
      String id = environment.getProperty("consumer.verify-id", "packaged-" + System.nanoTime());
      if (environment.getProperty("consumer.verify-submit", Boolean.class, true))
        service.submit(id);
      long timeoutSeconds =
          environment.getProperty("consumer.verify-timeout-seconds", Long.class, 30L);
      long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
      while (System.nanoTime() < deadline) {
        ConsumerRecord record = mongoTemplate.findById(id, ConsumerRecord.class);
        if (record != null && "executed".equals(record.state())) {
          System.out.println("RATCHET_MONGODB_CONSUMER_VERIFIED");
          return;
        }
        Thread.sleep(50);
      }
      throw new IllegalStateException("Packaged MongoDB consumer job did not execute");
    };
  }
}
