package run.ratchet.ri.security;

import java.lang.reflect.Method;
import org.objectweb.asm.Type;
import run.ratchet.store.entity.JobPayload;

/** Shared helpers for locating methods by name and descriptor on a class. */
class MethodLookup {

  private MethodLookup() {}

  /** Returns the first public method matching the payload's name and descriptor, or null. */
  static Method findMethod(Class<?> clazz, JobPayload payload) {
    for (Method m : clazz.getMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        return m;
      }
    }
    return null;
  }

  /**
   * Returns the first declared method (any visibility) matching the payload's name and descriptor,
   * or null.
   */
  static Method findDeclaredMethod(Class<?> clazz, JobPayload payload) {
    for (Method m : clazz.getDeclaredMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        return m;
      }
    }
    return null;
  }
}
