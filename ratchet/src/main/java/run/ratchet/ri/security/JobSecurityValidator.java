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
package run.ratchet.ri.security;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.jboss.logging.Logger;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.store.entity.JobPayload;

/**
 * Primary security gate for job execution. Enforces class-policy allowlisting, method visibility
 * (public only), and signature existence checks. Default-deny: any failure blocks execution.
 *
 * @see ClassPolicy
 * @see JobPayloadInputValidator for structural validation (non-security)
 */
@ApplicationScoped
public class JobSecurityValidator {

  private static final Logger log = Logger.getLogger(JobSecurityValidator.class);

  private final ClassPolicy classPolicy;
  private final ClassLoader applicationClassLoader;

  // CDI proxy
  protected JobSecurityValidator() {
    this.classPolicy = null;
    this.applicationClassLoader = null;
  }

  @Inject
  public JobSecurityValidator(ClassPolicy classPolicy) {
    this(classPolicy, Thread.currentThread().getContextClassLoader());
  }

  JobSecurityValidator(ClassPolicy classPolicy, ClassLoader applicationClassLoader) {
    this.classPolicy = classPolicy;
    this.applicationClassLoader =
        applicationClassLoader != null
            ? applicationClassLoader
            : JobSecurityValidator.class.getClassLoader();
  }

  /**
   * @throws SecurityException if the class or method fails policy or visibility checks
   */
  public void validate(JobPayload payload) {
    if (payload == null) {
      throw new SecurityException("Job payload cannot be null");
    }

    String targetClass = payload.target();
    if (targetClass == null || targetClass.isEmpty()) {
      throw new SecurityException("Job payload target class cannot be null or empty");
    }

    if (!classPolicy.isAllowed(targetClass)) {
      throw new SecurityException("Class " + targetClass + " is not allowed for job execution.");
    }

    Class<?> clazz;
    try {
      clazz = Class.forName(targetClass, false, applicationClassLoader);
    } catch (ClassNotFoundException e) {
      log.errorf(e, "Cannot load class %s for job execution", targetClass);
      throw new SecurityException(
          "Class " + targetClass + " cannot be loaded: " + e.getMessage(), e);
    }

    Method method = MethodLookup.findMethod(clazz, payload);
    if (method == null) {
      Method nonPublic = MethodLookup.findDeclaredMethod(clazz, payload);
      if (nonPublic != null) {
        String visibility =
            Modifier.isPrivate(nonPublic.getModifiers())
                ? "private"
                : Modifier.isProtected(nonPublic.getModifiers()) ? "protected" : "package-private";
        log.debugf(
            "Rejected non-public job method %s.%s%s (%s)",
            targetClass, payload.method(), payload.methodDescriptor(), visibility);
        throw new SecurityException("Only public methods can be scheduled as jobs.");
      }
      NoSuchMethodException missing =
          new NoSuchMethodException(
              "Method "
                  + payload.method()
                  + " with descriptor "
                  + payload.methodDescriptor()
                  + " not found in class "
                  + targetClass);
      throw new SecurityException(missing.getMessage(), missing);
    }

    int modifiers = method.getModifiers();
    if (!Modifier.isPublic(modifiers)) {
      log.debugf(
          "Rejected non-public job method %s.%s%s",
          targetClass, payload.method(), payload.methodDescriptor());
      throw new SecurityException("Only public methods can be scheduled as jobs.");
    }

    log.debugf("Validated: %s.%s", targetClass, payload.method());
  }
}
