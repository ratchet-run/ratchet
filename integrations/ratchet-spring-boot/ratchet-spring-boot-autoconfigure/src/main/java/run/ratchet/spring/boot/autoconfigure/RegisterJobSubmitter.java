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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class that submits Ratchet jobs, so Spring AOT processing registers it for lambda-capture
 * serialization and ships its bytecode into the native image.
 *
 * <p>Submitting an inline lambda such as {@code () -> service.work(value)} requires reading the
 * lambda body's bytecode at runtime to resolve the invocation it stands for. A native image ships
 * no class files by default, so Ratchet registers each submitting class explicitly.
 *
 * <p>Ratchet finds submitters automatically when a Spring bean declares a {@link
 * run.ratchet.api.JobSchedulerService} field or method parameter, which covers ordinary injection.
 * Annotate a class with {@code @RegisterJobSubmitter} when it submits jobs but obtains the
 * scheduler some other way — most commonly an application-context lookup, a lookup helper, or a
 * base class submitting on behalf of subclasses. Without it, such a class fails in native at
 * submission time with {@code IllegalStateException: Bytecode not found}.
 *
 * <p>This annotation only affects native-image builds; it is a no-op in JVM mode, where class files
 * are always on the classpath. Job targets submitted as method references need no registration,
 * because a method reference names its target directly instead of hiding it in a lambda body.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RegisterJobSubmitter {}
