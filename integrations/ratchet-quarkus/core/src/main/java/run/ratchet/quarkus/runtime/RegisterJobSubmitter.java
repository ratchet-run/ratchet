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
package run.ratchet.quarkus.runtime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import run.ratchet.api.JobSchedulerService;

/**
 * Registers a dependency class for native lambda serialization and includes its bytecode as a
 * resource. Annotate the class that lexically contains the submitted lambda or method reference.
 *
 * <p>The extension registers all application-index classes automatically. For indexed dependencies,
 * it also discovers classes declaring a {@link JobSchedulerService} field or method parameter,
 * including {@code Instance} and {@code Provider} wrappers. Use this annotation for other
 * dependency submitters, such as classes using a programmatic lookup, an inherited scheduler, or a
 * lambda in a nested class. The dependency must be indexed by Quarkus for the annotation to be
 * discovered.
 *
 * <p>Both method references and inline lambdas need lambda-serialization metadata in native images.
 * Inline lambdas additionally need their containing class's bytecode for invocation analysis. This
 * annotation supplies both; it does not register dependency job targets for reflective execution.
 * It has no effect in JVM mode.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RegisterJobSubmitter {}
