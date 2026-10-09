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
/**
 * Constructor carriers for factory-produced transactional core beans. Micronaut analyzes the
 * declared factory return type and requires its primary constructor to have no arguments. Core
 * protected no-arg constructors are inaccessible from this package; an explicit public
 * {@code @Creator} constructor selects the safe proxy constructor instead of a collaborator
 * constructor. Factories create the actual target through a pass-through constructor with real
 * collaborators. Allowing null proxy constructor arguments is unsafe for CDI Instance parameters.
 *
 * <p>Carriers copy class-level Jakarta transaction metadata exactly. Method-only classes keep no
 * class default and use the factory's RatchetTransactionBoundary marker. BatchCompletionTransaction
 * already has an implicit public no-arg constructor and needs no carrier.
 *
 * <p>The package is vetoed: a class-level transaction annotation would otherwise make each carrier
 * a bean of its own, built through the no-arg constructor with null collaborators, competing with
 * the factory-produced bean.
 */
@Vetoed
package run.ratchet.micronaut.internal.tx;

import io.micronaut.core.annotation.Vetoed;
