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
package run.ratchet.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Commits the job body and its success record in one transaction. The body's database writes and
 * the job's SUCCEEDED state commit or roll back together.
 *
 * <p>Requires a store with transactional completion support: MySQL, PostgreSQL, Oracle or SQL
 * Server. Submission fails on other stores. The guarantee covers only writes through the same
 * transaction manager and datasource as Ratchet's store: on Spring, the manager Ratchet selects; on
 * Micronaut, the {@code ratchet.jpa.datasource} datasource. Writes through other datasources commit
 * separately.
 *
 * <p>Checkpoints and continuations throw {@link IllegalStateException}; progress is allowed. The
 * transaction uses the container's or manager's default timeout. Each running transactional job
 * holds a pooled connection for the whole body.
 */
@Incubating
@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface TransactionalJob {}
