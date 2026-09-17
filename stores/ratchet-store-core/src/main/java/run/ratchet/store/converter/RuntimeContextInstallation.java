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
package run.ratchet.store.converter;

import java.util.Collection;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.spi.PayloadMaskingPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.util.PayloadMaskingPolicyHolder;

/** Owns converter configuration for one application runtime in this class loader. */
public final class RuntimeContextInstallation implements AutoCloseable {
  private static RuntimeContextInstallation active;
  private final PayloadSerializer previousSerializer;
  private final PayloadMaskingPolicy previousMasking;
  private final Runnable restoreEncryption;
  private BooleanSupplier quiescent = () -> true;
  private boolean mutating;
  private boolean closeRequested;
  private boolean closed;

  private RuntimeContextInstallation() {
    synchronized (RuntimeContextInstallation.class) {
      if (active != null) active.releaseIfQuiescent();
      if (active != null) {
        throw new IllegalStateException(
            "Another Ratchet runtime already owns converter configuration in this class loader;"
                + " close it and await its active executions before starting another application"
                + " context");
      }
      previousSerializer = PayloadSerializerHolder.get();
      previousMasking = PayloadMaskingPolicyHolder.get();
      restoreEncryption = EncryptionHolder.snapshotRestorer();
      active = this;
    }
  }

  /** Claims ownership before a framework installs configuration in separate startup phases. */
  public static RuntimeContextInstallation claim() {
    return new RuntimeContextInstallation();
  }

  public RuntimeContextInstallation(
      PayloadSerializer serializer,
      PayloadMaskingPolicy maskingPolicy,
      Collection<PayloadEncryption> encryptionEngines,
      String writeAlgorithm,
      KeyProvider keyProvider,
      boolean globallyEncrypted) {
    this();
    try {
      Objects.requireNonNull(serializer, "serializer");
      configureEncryption(encryptionEngines, writeAlgorithm, keyProvider, globallyEncrypted);
      setSerializer(serializer);
      setMaskingPolicy(maskingPolicy);
    } catch (RuntimeException | Error failure) {
      close();
      throw failure;
    }
  }

  public void setSerializer(PayloadSerializer serializer) {
    mutate(() -> PayloadSerializerHolder.set(serializer));
  }

  public void setMaskingPolicy(PayloadMaskingPolicy policy) {
    mutate(() -> PayloadMaskingPolicyHolder.set(policy));
  }

  public void configureEncryption(
      Collection<PayloadEncryption> engines, String algorithm, KeyProvider keys, boolean global) {
    mutate(
        () -> {
          if ((engines != null && !engines.isEmpty()) || global || keys != null) {
            EncryptionHolder.install(engines, algorithm, keys, global);
          } else {
            EncryptionHolder.disable();
          }
        });
  }

  /** Keeps the ownership fence after bounded shutdown while canceled runners still execute. */
  public void releaseWhen(BooleanSupplier executionsQuiescent) {
    synchronized (RuntimeContextInstallation.class) {
      requireOwner();
      quiescent = Objects.requireNonNull(executionsQuiescent, "executionsQuiescent");
    }
  }

  private void mutate(Runnable mutation) {
    synchronized (RuntimeContextInstallation.class) {
      requireOwner();
      mutating = true;
      try {
        mutation.run();
      } finally {
        mutating = false;
      }
    }
  }

  private void requireOwner() {
    if (closed || closeRequested || active != this) {
      throw new IllegalStateException(
          "Ratchet converter installation no longer owns configuration");
    }
  }

  /** Guards legacy holder mutation; only the current installation can authorize writes. */
  public static synchronized void checkUnowned() {
    if (active != null && !active.mutating) {
      throw new IllegalStateException(
          "Converter configuration is owned by an active Ratchet runtime");
    }
  }

  @Override
  public void close() {
    synchronized (RuntimeContextInstallation.class) {
      if (closed) return;
      closeRequested = true;
      releaseIfQuiescent();
    }
  }

  /** Called after the last actual runner exits; normal idle periods do not close the owner. */
  public void releaseIfRequested() {
    synchronized (RuntimeContextInstallation.class) {
      releaseIfQuiescent();
    }
  }

  private void releaseIfQuiescent() {
    if (!closeRequested || closed || !quiescent.getAsBoolean()) return;
    if (active != this) throw new IllegalStateException("Ratchet converter ownership was lost");
    closed = true;
    active = null;
    PayloadSerializerHolder.set(previousSerializer);
    PayloadMaskingPolicyHolder.set(previousMasking);
    restoreEncryption.run();
  }
}
