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
package run.ratchet.quarkus.runtime.graal;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextOption;
import io.netty.handler.ssl.SslProvider;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

/**
 * Backports Quarkus' JDK SSL handling for Netty's newer client overload to Quarkus 3.20.
 *
 * @see <a
 *     href="https://github.com/quarkusio/quarkus/blob/3.27.5.2/extensions/netty/runtime/src/main/java/io/quarkus/netty/runtime/graal/NettySubstitutions.java">Quarkus
 *     Netty substitutions</a>
 */
@TargetClass(
    className = "io.netty.handler.ssl.SslContext",
    onlyWith = MissingNettyClientSslSubstitution.class)
final class NettyClientSslCompatibility {
  @Substitute
  @SafeVarargs
  private static SslContext newClientContextInternal(
      SslProvider provider,
      Provider sslContextProvider,
      X509Certificate[] trustCert,
      TrustManagerFactory trustManagerFactory,
      X509Certificate[] keyCertChain,
      PrivateKey key,
      String keyPassword,
      KeyManagerFactory keyManagerFactory,
      Iterable<String> ciphers,
      CipherSuiteFilter cipherFilter,
      ApplicationProtocolConfig apn,
      String[] protocols,
      long sessionCacheSize,
      long sessionTimeout,
      boolean startTls,
      boolean enableOcsp,
      SecureRandom secureRandom,
      String keyStoreType,
      String endpointIdentificationAlgorithm,
      Map.Entry<SslContextOption<?>, Object>... options)
      throws SSLException {
    // Match Quarkus 3.27's backport of this overload, including its JDK-only SSL behavior.
    if (enableOcsp) {
      throw new IllegalArgumentException(
          "OCSP is not supported with this SslProvider: " + provider);
    }
    return (SslContext)
        (Object)
            new NettyJdkSslClientContext(
                sslContextProvider,
                trustCert,
                trustManagerFactory,
                keyCertChain,
                key,
                keyPassword,
                keyManagerFactory,
                ciphers,
                cipherFilter,
                apn,
                protocols,
                sessionCacheSize,
                sessionTimeout,
                secureRandom,
                keyStoreType,
                endpointIdentificationAlgorithm,
                new NettyResumptionController());
  }
}

@TargetClass(
    className = "io.netty.handler.ssl.JdkSslClientContext",
    onlyWith = MissingNettyClientSslSubstitution.class)
final class NettyJdkSslClientContext {
  @Alias
  NettyJdkSslClientContext(
      Provider sslContextProvider,
      X509Certificate[] trustCert,
      TrustManagerFactory trustManagerFactory,
      X509Certificate[] keyCertChain,
      PrivateKey key,
      String keyPassword,
      KeyManagerFactory keyManagerFactory,
      Iterable<String> ciphers,
      CipherSuiteFilter cipherFilter,
      ApplicationProtocolConfig apn,
      String[] protocols,
      long sessionCacheSize,
      long sessionTimeout,
      SecureRandom secureRandom,
      String keyStoreType,
      String endpointIdentificationAlgorithm,
      NettyResumptionController resumptionController)
      throws SSLException {}
}

@TargetClass(
    className = "io.netty.handler.ssl.ResumptionController",
    onlyWith = MissingNettyClientSslSubstitution.class)
final class NettyResumptionController {
  @Alias
  NettyResumptionController() {}
}
