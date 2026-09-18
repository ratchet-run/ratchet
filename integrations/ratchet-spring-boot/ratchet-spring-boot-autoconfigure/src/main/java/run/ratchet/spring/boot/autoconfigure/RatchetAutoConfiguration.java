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
package run.ratchet.spring.boot.autoconfigure;

import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.transaction.PlatformTransactionManager;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import run.ratchet.ri.cdi.NoOpClusterCoordinator;
import run.ratchet.ri.cdi.NoOpTracingCollector;
import run.ratchet.ri.cdi.ReferenceEncryptionFactory;
import run.ratchet.ri.cdi.internal.JsonbPayloadSerializer;
import run.ratchet.ri.security.PackagePrefixClassPolicy;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NoOpMetricsCollector;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.spi.PayloadMaskingPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.converter.RuntimeContextInstallation;

/** Framework adapters shared by SQL and MongoDB starters. */
@AutoConfiguration
@ConditionalOnProperty(
    prefix = "ratchet",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@EnableConfigurationProperties(RatchetProperties.class)
public class RatchetAutoConfiguration {
  @Bean
  @ConditionalOnMissingBean
  RatchetOptions ratchetOptions(Environment environment, RatchetProperties properties) {
    return RatchetOptionsFactory.builderFromEnvironment(
            (property, variable) -> {
              String value = environment.getProperty(property);
              if (value == null
                  && property.equals("ratchet.worker.default-threading-mode")
                  && virtualThreadsEnabled(environment)) {
                value = "virtual";
              }
              return Optional.ofNullable(value);
            })
        .schema(schema -> schema.autoMigrate(properties.getSchema().isAutoMigrate()))
        .encryption(
            encryption ->
                encryption
                    .enabled(
                        environment.getProperty("ratchet.encryption.enabled", Boolean.class, false))
                    .writeAlgorithm(environment.getProperty("ratchet.encryption.write-algorithm")))
        .build();
  }

  static boolean virtualThreadsEnabled(Environment environment) {
    return environment.getProperty("spring.threads.virtual.enabled", Boolean.class, false)
        && Runtime.version().feature() >= 21;
  }

  @Bean
  @ConditionalOnMissingBean
  Clock ratchetClock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean
  MetricsCollector ratchetMetricsCollector() {
    return new NoOpMetricsCollector();
  }

  @Bean
  @ConditionalOnMissingBean
  TracingCollector ratchetTracingCollector() {
    return new NoOpTracingCollector();
  }

  @Bean
  @ConditionalOnMissingBean
  ClusterCoordinator ratchetClusterCoordinator() {
    return new NoOpClusterCoordinator();
  }

  @Bean
  @ConditionalOnMissingBean
  PayloadSerializer ratchetPayloadSerializer() {
    return new JsonbPayloadSerializer();
  }

  @Bean(destroyMethod = "close")
  RuntimeContextInstallation ratchetRuntimeInstallation(
      PayloadSerializer serializer,
      ObjectProvider<PayloadMaskingPolicy> masking,
      ObjectProvider<PayloadEncryption> encryption,
      ObjectProvider<KeyProvider> keyProvider,
      RatchetOptions options,
      Environment environment) {
    var engines = encryption.orderedStream().toList();
    KeyProvider keys = keyProvider.getIfAvailable();
    if (engines.isEmpty() && keys == null) {
      // Converter installation precedes the database-backed node service. An independent runtime
      // nonce identity avoids that dependency cycle while keeping key parsing in the shared
      // factory.
      String nonceIdentity = options.node().nodeId();
      if (nonceIdentity == null || nonceIdentity.isBlank())
        nonceIdentity = UUID.randomUUID().toString();
      var reference =
          ReferenceEncryptionFactory.build(
              environment.getProperty("ratchet.encryption.keys"),
              environment.getProperty("ratchet.encryption.current-key"),
              ReferenceEncryptionFactory.nodeEntropy(nonceIdentity));
      if (reference.isPresent()) {
        engines = List.of(reference.get().engine());
        keys = reference.get().keyProvider();
      }
    }
    String algorithm = options.encryption().writeAlgorithm();
    if ((algorithm == null || algorithm.isBlank()) && engines.size() > 1)
      throw new IllegalStateException(
          "Multiple PayloadEncryption beans are installed; set ratchet.encryption.write-algorithm to one of "
              + engines.stream().map(PayloadEncryption::algorithmId).toList());
    if ((algorithm == null || algorithm.isBlank()) && engines.size() == 1)
      algorithm = engines.get(0).algorithmId();
    return new RuntimeContextInstallation(
        serializer,
        masking.getIfAvailable(),
        engines,
        algorithm,
        keys,
        options.encryption().enabled());
  }

  @Bean
  @ConditionalOnMissingBean
  AfterCommitRegistrar ratchetAfterCommitRegistrar(
      ObjectProvider<PlatformTransactionManager> managers) {
    return new SpringAfterCommitRegistrar(managers::getObject);
  }

  @Bean
  @ConditionalOnMissingBean
  BeanResolver ratchetBeanResolver(ConfigurableListableBeanFactory beanFactory) {
    return new SpringBeanResolver(beanFactory);
  }

  @Bean
  @ConditionalOnMissingBean
  ClassPolicy ratchetClassPolicy(
      ConfigurableListableBeanFactory beanFactory,
      RatchetProperties properties,
      RatchetOptions options) {
    var packages = properties.getAllowedPackages();
    if (packages == null) {
      packages = new LinkedHashSet<>();
      if (AutoConfigurationPackages.has(beanFactory))
        packages.addAll(AutoConfigurationPackages.get(beanFactory));
    }
    if (packages.isEmpty() && !options.security().allowEmptyClassPolicy()) {
      throw new IllegalStateException(
          "Ratchet cannot infer job packages. Configure ratchet.allowed-packages or provide a"
              + " ClassPolicy bean.");
    }
    return new PackagePrefixClassPolicy(packages, properties.getAllowedResultTypePackages());
  }
}
