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

import io.quarkus.hibernate.orm.PersistenceUnit;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import jakarta.persistence.EntityManager;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/**
 * Binds Ratchet's SQL stores to a dedicated {@value #PERSISTENCE_UNIT_NAME} persistence unit instead
 * of the application's default one.
 *
 * <p>Ratchet ships its entities in {@code run.ratchet.store.entity} and a {@code META-INF/orm.xml}
 * that maps them along with the store's {@code @Converter} classes. On Quarkus the default
 * persistence unit both auto-scans the indexed jar for those converters <em>and</em> reads them from
 * orm.xml, which aborts the boot with "AttributeConverter registered multiple times". Giving Ratchet
 * its own unit — scoped to the entity package only — leaves orm.xml as the converters' single source
 * and, more importantly, isolates Ratchet's schema settings (notably {@code generation=none}) from
 * whatever the application does to its default unit.
 *
 * <p>The application declares the unit in {@code application.properties}:
 *
 * <pre>{@code
 * quarkus.hibernate-orm."ratchet".packages=run.ratchet.store.entity
 * quarkus.hibernate-orm."ratchet".database.generation=none
 * }</pre>
 *
 * <p>Quarkus 3.20 cannot have an extension materialize a named unit from build-time defaults, so the
 * two lines above are required. The unit uses the default datasource unless {@code
 * quarkus.hibernate-orm."ratchet".datasource} names another. Because any named unit suppresses
 * auto-creation of the default unit, an application that also has its own entities must declare its
 * default unit explicitly (e.g. {@code quarkus.hibernate-orm.packages=...}).
 *
 * <p>This provider is {@code @Alternative} with {@code @Priority(APPLICATION)} so the extension
 * enables it over the store modules' default {@link RatchetEntityManagerProvider} (which uses the
 * unnamed persistence context). An application that wants Ratchet on a different unit can override it
 * with its own {@code @Alternative @Priority(APPLICATION + 1)} provider.
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class QuarkusRatchetEntityManagerProvider implements RatchetEntityManagerProvider {

  /** Name of the persistence unit Ratchet's stores bind to on Quarkus. */
  public static final String PERSISTENCE_UNIT_NAME = "ratchet";

  @Inject
  @PersistenceUnit(PERSISTENCE_UNIT_NAME)
  EntityManager entityManager;

  @Override
  public EntityManager getEntityManager() {
    return entityManager;
  }
}
