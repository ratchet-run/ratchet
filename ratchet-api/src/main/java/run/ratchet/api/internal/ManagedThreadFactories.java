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
package run.ratchet.api.internal;

import java.util.concurrent.ThreadFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/** Shared lookup for container-managed worker threads. */
public final class ManagedThreadFactories {
  private ManagedThreadFactories() {}

  /** Resolves a managed factory without falling back to unmanaged threads. */
  public static ThreadFactory resolve(String jndiName, String purpose) {
    try {
      return resolve(new InitialContext(), jndiName, purpose);
    } catch (NamingException e) {
      throw lookupFailure(jndiName, purpose, e);
    }
  }

  /** Resolves a managed factory through the supplied naming context. */
  public static ThreadFactory resolve(Context context, String jndiName, String purpose) {
    try {
      Object bound = context.lookup(jndiName);
      if (!(bound instanceof ThreadFactory factory)) {
        throw new IllegalStateException(
            purpose
                + " expected "
                + jndiName
                + " to be a java.util.concurrent.ThreadFactory. Configure"
                + " ratchet.worker.thread-factory-jndi to name a managed thread factory");
      }
      return factory;
    } catch (NamingException e) {
      throw lookupFailure(jndiName, purpose, e);
    }
  }

  private static IllegalStateException lookupFailure(
      String jndiName, String purpose, NamingException e) {
    return new IllegalStateException(
        purpose
            + " could not resolve "
            + jndiName
            + " from JNDI. Configure ratchet.worker.thread-factory-jndi if the managed thread"
            + " factory has a different name. Outside a Jakarta EE 10+ container, use"
            + " StandaloneExecutorProvider and CoordinatorThreading.standalone(...).",
        e);
  }
}
