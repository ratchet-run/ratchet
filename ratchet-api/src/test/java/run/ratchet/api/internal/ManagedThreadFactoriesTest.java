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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Hashtable;
import java.util.concurrent.ThreadFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

@ResourceLock("javax.naming.Context.INITIAL_CONTEXT_FACTORY")
class ManagedThreadFactoriesTest {
  @Test
  void missingBindingExplainsConfigurationAndStandaloneMode() throws Exception {
    withBinding(
        null,
        () -> {
          IllegalStateException failure =
              assertThrows(
                  IllegalStateException.class,
                  () -> ManagedThreadFactories.resolve("java:test/factory", "Release executor"));
          assertTrue(failure.getMessage().contains("ratchet.worker.thread-factory-jndi"));
          assertTrue(failure.getMessage().contains("StandaloneExecutorProvider"));
          assertTrue(failure.getCause() instanceof NamingException);
        });
  }

  @Test
  void rejectsBindingThatIsNotAThreadFactory() throws Exception {
    withBinding(
        "wrong type",
        () -> {
          IllegalStateException failure =
              assertThrows(
                  IllegalStateException.class,
                  () -> ManagedThreadFactories.resolve("java:test/factory", "Release executor"));
          assertTrue(failure.getMessage().contains("ThreadFactory"));
        });
  }

  @Test
  void returnsBoundFactory() throws Exception {
    ThreadFactory factory = Thread::new;
    withBinding(
        factory,
        () ->
            assertSame(
                factory, ManagedThreadFactories.resolve("java:test/factory", "Release executor")));
  }

  @Test
  void returnsFactoryFromSuppliedContext() throws Exception {
    ThreadFactory factory = Thread::new;
    Context context =
        new InitialContext(true) {
          @Override
          public Object lookup(String name) {
            assertEquals("java:test/factory", name);
            return factory;
          }
        };
    assertSame(
        factory, ManagedThreadFactories.resolve(context, "java:test/factory", "Release executor"));
  }

  private void withBinding(Object binding, Runnable check) throws Exception {
    String previous = System.getProperty(Context.INITIAL_CONTEXT_FACTORY);
    Factory.binding = binding;
    System.setProperty(Context.INITIAL_CONTEXT_FACTORY, Factory.class.getName());
    try {
      check.run();
    } finally {
      Factory.binding = null;
      if (previous == null) System.clearProperty(Context.INITIAL_CONTEXT_FACTORY);
      else System.setProperty(Context.INITIAL_CONTEXT_FACTORY, previous);
    }
  }

  public static final class Factory implements InitialContextFactory {
    static Object binding;

    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
      return new InitialContext(true) {
        @Override
        public Object lookup(String name) throws NamingException {
          if (binding == null) throw new NameNotFoundException(name);
          return binding;
        }
      };
    }
  }
}
