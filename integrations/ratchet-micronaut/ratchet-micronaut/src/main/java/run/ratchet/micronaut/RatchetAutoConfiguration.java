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
package run.ratchet.micronaut;

import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.transaction.TransactionOperations;
import jakarta.inject.Singleton;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import run.ratchet.api.internal.RatchetConfigKeys;
import run.ratchet.micronaut.internal.NoTransactionOperations;
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

/** Framework adapters shared by SQL and MongoDB applications. */
@Factory
@Requires(property = "ratchet.enabled", notEquals = "false")
public class RatchetAutoConfiguration {
  @Singleton
  @Requires(missingBeans = RatchetOptions.class)
  RatchetOptions ratchetOptions(Environment environment) {
    return RatchetOptionsFactory.builderFromEnvironment(
            (property, variable) -> {
              var value = environment.getProperty(property, String.class);
              if (value.isEmpty()
                  && property.equals("ratchet.worker.default-threading-mode")
                  && virtualThreadsEnabled(environment)) return Optional.of("virtual");
              if (value.isEmpty() && property.equals(RatchetConfigKeys.SCHEMA_AUTO_MIGRATE.name()))
                return Optional.of("true");
              return value;
            })
        .encryption(
            encryption ->
                encryption
                    .enabled(
                        environment
                            .getProperty("ratchet.encryption.enabled", Boolean.class)
                            .orElse(false))
                    .writeAlgorithm(
                        environment
                            .getProperty("ratchet.encryption.write-algorithm", String.class)
                            .orElse(null)))
        .build();
  }

  static boolean virtualThreadsEnabled(Environment environment) {
    return environment
            .getProperty("micronaut.executors.default.virtual", Boolean.class)
            .orElse(false)
        && Runtime.version().feature() >= 21;
  }

  @Singleton
  @Requires(missingBeans = Clock.class)
  Clock ratchetClock() {
    return Clock.systemUTC();
  }

  @Singleton
  @Requires(missingBeans = MetricsCollector.class)
  MetricsCollector ratchetMetricsCollector(BeanContext context) {
    return MicronautObservabilityConditions.metricsAvailable(context)
        ? MicrometerCollectors.metrics(context)
        : new NoOpMetricsCollector();
  }

  @Singleton
  @Requires(missingBeans = TracingCollector.class)
  TracingCollector ratchetTracingCollector(BeanContext context) {
    return MicronautObservabilityConditions.tracingAvailable(context)
        ? MicrometerCollectors.tracing(context)
        : new NoOpTracingCollector();
  }

  @Singleton
  @Requires(missingBeans = ClusterCoordinator.class)
  ClusterCoordinator ratchetClusterCoordinator() {
    return new NoOpClusterCoordinator();
  }

  @Singleton
  @Requires(missingBeans = PayloadSerializer.class)
  PayloadSerializer ratchetPayloadSerializer() {
    return new JsonbPayloadSerializer();
  }

  @Context
  @Bean(preDestroy = "close")
  @Requires(missingBeans = RuntimeContextInstallation.class)
  RuntimeContextInstallation ratchetRuntimeInstallation(
      BeanContext context,
      PayloadSerializer serializer,
      BeanProvider<PayloadMaskingPolicy> masking,
      BeanProvider<PayloadEncryption> encryption,
      BeanProvider<KeyProvider> keyProvider,
      RatchetOptions options,
      Environment environment) {
    var engines = encryption.stream().toList();
    KeyProvider keys = keyProvider.orElse(null);
    if (engines.isEmpty() && keys == null) {
      String nonceIdentity = options.node().nodeId();
      if (nonceIdentity == null || nonceIdentity.isBlank())
        nonceIdentity = UUID.randomUUID().toString();
      var reference =
          ReferenceEncryptionFactory.build(
              environment.getProperty("ratchet.encryption.keys", String.class).orElse(null),
              environment.getProperty("ratchet.encryption.current-key", String.class).orElse(null),
              ReferenceEncryptionFactory.nodeEntropy(nonceIdentity));
      if (reference.isPresent()) {
        engines = List.of(reference.get().engine());
        keys = reference.get().keyProvider();
      }
    }
    var installedEngines = engines;
    var installedKeys = keys;
    return MicronautRuntimeOwnership.install(
        context,
        () ->
            new RuntimeContextInstallation(
                serializer,
                masking.orElse(null),
                installedEngines,
                options.encryption().writeAlgorithm(),
                installedKeys,
                options.encryption().enabled()));
  }

  @Singleton
  @Requires(missingBeans = AfterCommitRegistrar.class)
  AfterCommitRegistrar ratchetAfterCommitRegistrar(
      BeanProvider<TransactionOperations<?>> operations) {
    return new MicronautAfterCommitRegistrar(() -> operations.orElse(null));
  }

  @Singleton
  @Requires(missingBeans = BeanResolver.class)
  BeanResolver ratchetBeanResolver(BeanContext context) {
    return new MicronautBeanResolver(context);
  }

  @Singleton
  @Requires(missingBeans = ClassPolicy.class)
  ClassPolicy ratchetClassPolicy(RatchetProperties properties, RatchetOptions options) {
    Set<String> packages = properties.getAllowedPackages();
    if (packages == null) packages = Set.of();
    if (packages.isEmpty() && !options.security().allowEmptyClassPolicy())
      throw new IllegalStateException(
          "Ratchet cannot infer job packages. Configure ratchet.allowed-packages or provide a"
              + " ClassPolicy bean.");
    return new PackagePrefixClassPolicy(packages, properties.getAllowedResultTypePackages());
  }

  /** No synthetic active transaction: Mongo-only callbacks take the immediate, non-tx path. */
  @Singleton
  @Requires(missingBeans = TransactionOperations.class)
  // Any real transaction manager replaces this concrete no-transaction adapter.
  NoTransactionOperations ratchetNoTransactionOperations() {
    return new NoTransactionOperations();
  }
}
