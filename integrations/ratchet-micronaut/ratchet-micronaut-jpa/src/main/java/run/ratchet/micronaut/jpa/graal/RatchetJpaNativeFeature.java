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
package run.ratchet.micronaut.jpa.graal;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeProxyCreation;
import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.graalvm.nativeimage.hosted.RuntimeResourceAccess;
import run.ratchet.micronaut.graal.NativeImageSupport;
import run.ratchet.store.schema.RatchetJpaModel;

/** Presence-guarded SQL store, persistence model and Hibernate XML mapping metadata. */
public final class RatchetJpaNativeFeature implements Feature {
  record Store(String vendor, String title) {
    String interfaceName() {
      return "run.ratchet.store." + vendor + "." + title + "JobStore";
    }

    String implementationName() {
      return interfaceName() + "Impl";
    }

    String adapterName() {
      return "run.ratchet.micronaut.jpa.SqlStoreVendor$" + title + "Vendor";
    }
  }

  static final List<Store> STORES =
      List.of(
          new Store("postgresql", "Postgresql"),
          new Store("mysql", "Mysql"),
          new Store("oracle", "Oracle"),
          new Store("sqlserver", "Sqlserver"));
  static final List<String> EXTRA_MODEL_TYPES =
      List.of(
          "run.ratchet.store.id.UuidV7EntityListener",
          "run.ratchet.store.converter.InstantAttributeConverter");

  @Override
  public void beforeAnalysis(BeforeAnalysisAccess access) {
    for (String name : RatchetJpaModel.ENTITY_CLASS_NAMES)
      NativeImageSupport.registerType(access, name);
    for (String name : RatchetJpaModel.CONVERTER_CLASS_NAMES)
      NativeImageSupport.registerType(access, name);
    for (String name : EXTRA_MODEL_TYPES) NativeImageSupport.registerType(access, name);
    Class<?> entityManager = access.findClassByName("jakarta.persistence.EntityManager");
    if (entityManager != null) {
      RuntimeReflection.registerAllMethods(entityManager);
      RuntimeReflection.register(entityManager.getMethods());
      // RatchetJpaInfrastructure.entityManager passes precisely this singleton interface array.
      RuntimeProxyCreation.register(entityManager);
    }
    NativeImageSupport.registerResources(
        access, getClass().getModule(), name -> name.equals("META-INF/orm.xml"));
    int stores = 0;
    Set<Path> storePaths = new LinkedHashSet<>();
    for (Store store : STORES) {
      Class<?> composite = access.findClassByName(store.interfaceName());
      if (composite == null) continue;
      stores++;
      String storeResource = store.interfaceName().replace('.', '/') + ".class";
      for (Path path : access.getApplicationClassPath()) {
        NativeImageSupport.visitEntries(
            List.of(path), storeResource::equals, (name, bytes) -> storePaths.add(path));
      }
      NativeImageSupport.registerType(access, store.adapterName());
      Class<?> implementation = NativeImageSupport.registerType(access, store.implementationName());
      if (implementation != null) registerInterfaces(implementation);
      registerInterfaces(composite);
      // RatchetTransactionalStoreProxy.createProxy passes {compositeType}, not its parents.
      RuntimeProxyCreation.register(composite);
      NativeImageSupport.registerResources(
          access,
          getClass().getModule(),
          name ->
              name.equals("META-INF/orm-" + store.vendor() + ".xml")
                  || (name.startsWith("META-INF/ratchet/hibernate/orm-" + store.vendor())
                      && name.endsWith(".xml")));
      if (store.vendor().equals("oracle")) registerOracleLobs(access);
    }
    if (stores > 0) {
      NativeImageSupport.visitEntries(
          List.copyOf(storePaths),
          name -> name.startsWith("ddl/migrations/"),
          (name, bytes) -> RuntimeResourceAccess.addResource(getClass().getModule(), name, bytes));
      registerHibernateXml(access);
    }
    System.out.println(
        "[ratchet-native-jpa] registered " + stores + " SQL stores and persistence metadata");
  }

  private static void registerInterfaces(Class<?> type) {
    if (type.isInterface()) {
      RuntimeReflection.register(type);
      RuntimeReflection.registerAllDeclaredMethods(type);
      NativeImageSupport.registerMembers(type);
    }
    for (Class<?> implemented : type.getInterfaces()) registerInterfaces(implemented);
    Class<?> superclass = type.getSuperclass();
    if (superclass != null && superclass != Object.class) {
      RuntimeReflection.registerAllDeclaredMethods(superclass);
      NativeImageSupport.registerMembers(superclass);
      registerInterfaces(superclass);
    }
  }

  private static void registerHibernateXml(BeforeAnalysisAccess access) {
    if (access.findClassByName("org.hibernate.jpa.HibernatePersistenceProvider") == null) return;
    // Spring HibernateJpaMappings.registerAotHints: JAXB adapters used by XML mappings.
    for (String name :
        List.of("CollapsedStringAdapter", "NormalizedStringAdapter", "HexBinaryAdapter")) {
      NativeImageSupport.registerType(access, "jakarta.xml.bind.annotation.adapters." + name);
    }
    // Hibernate 6.6 binds orm.xml through the JAXB runtime, which wraps each JAXB annotation it
    // reads in a JDK proxy that also implements Locatable.
    Class<?> locatable =
        access.findClassByName("org.glassfish.jaxb.core.v2.model.annotation.Locatable");
    if (locatable != null) {
      NativeImageSupport.visitEntries(
          access.getApplicationClassPath(),
          name -> name.startsWith("jakarta/xml/bind/annotation/") && name.endsWith(".class"),
          (resource, bytes) -> {
            Class<?> type =
                access.findClassByName(
                    resource.substring(0, resource.length() - 6).replace('/', '.'));
            if (type != null && type.isAnnotation()) {
              RuntimeReflection.register(type.getMethods());
              RuntimeProxyCreation.register(type, locatable);
            }
          });
      // The JAXB runtime builds its context reflectively: the factory, the navigator singleton
      // and one property class per mapped field shape.
      NativeImageSupport.registerType(access, "org.glassfish.jaxb.runtime.v2.JAXBContextFactory");
      NativeImageSupport.registerType(
          access, "org.glassfish.jaxb.core.v2.model.nav.ReflectionNavigator");
      // JAXB fills list properties through the no-argument ArrayList constructor and reads XML
      // through the JDK's StAX factories.
      RuntimeReflection.register(ArrayList.class.getDeclaredConstructors());
      NativeImageSupport.registerType(access, "com.sun.xml.internal.stream.XMLInputFactoryImpl");
      NativeImageSupport.registerType(
          access, "com.sun.xml.internal.stream.events.XMLEventFactoryImpl");
      NativeImageSupport.visitEntries(
          access.getApplicationClassPath(),
          name ->
              name.startsWith("org/glassfish/jaxb/runtime/v2/runtime/property/")
                  && name.endsWith(".class"),
          (resource, bytes) ->
              NativeImageSupport.registerType(
                  access, resource.substring(0, resource.length() - 6).replace('/', '.')));
    }
    // Spring HibernateJpaMappings.registerAotHints: schemas/DTDs loaded by Hibernate XML binding.
    NativeImageSupport.registerResources(
        access,
        RatchetJpaNativeFeature.class.getModule(),
        name ->
            name.startsWith("org/hibernate/") && (name.endsWith(".xsd") || name.endsWith(".dtd")));
    // Spring HibernateJpaMappings.registerAotHints: UUID array binding and generated logger
    // constructors.
    RuntimeReflection.register(UUID[].class);
    NativeImageSupport.visitEntries(
        access.getApplicationClassPath(),
        name ->
            name.endsWith(".class")
                && ((name.startsWith("org/hibernate/") && name.contains("_$logger"))
                    || name.startsWith("org/hibernate/boot/jaxb/mapping/")
                    || name.startsWith("org/hibernate/boot/jaxb/hbm/")
                    || name.startsWith("org/hibernate/boot/jaxb/cfg/")
                    || name.startsWith("jakarta/persistence/")
                    || name.startsWith("org/hibernate/annotations/")),
        (resource, bytes) -> {
          String name = resource.substring(0, resource.length() - 6).replace('/', '.');
          Class<?> type = access.findClassByName(name);
          if (type == null) return;
          if (resource.contains("_$logger")) {
            // jboss-logging finds the implementation by name, so the class itself must be
            // registered.
            RuntimeReflection.register(type);
            RuntimeReflection.registerAllDeclaredConstructors(type);
            NativeImageSupport.registerMembers(type);
            // jboss-logging first looks up locale-specific implementations that do not exist.
            if (name.endsWith("_$logger")) {
              RuntimeReflection.registerClassLookup(name + "_en");
              RuntimeReflection.registerClassLookup(name + "_en_US");
            }
          } else if (resource.startsWith("org/hibernate/boot/jaxb/")) {
            // Spring HibernateJpaMappings.registerAotHints: JAXB model constructors, methods and
            // fields.
            NativeImageSupport.registerType(access, name);
          } else if (type.isAnnotation()) {
            // Spring HibernateJpaMappings.registerAotHints: annotations synthesized from XML
            // overrides.
            RuntimeReflection.registerAllDeclaredMethods(type);
            NativeImageSupport.registerMembers(type);
            RuntimeProxyCreation.register(type);
          }
        });
  }

  private static void registerOracleLobs(BeforeAnalysisAccess access) {
    // Spring HibernateJpaMappings.registerLobHints: exact Hibernate 6/7 Oracle LOB proxy arrays.
    String root =
        access.findClassByName("org.hibernate.engine.jdbc.proxy.WrappedClob") != null
            ? "org.hibernate.engine.jdbc.proxy."
            : "org.hibernate.engine.jdbc.";
    for (String name : List.of("Blob", "Clob", "NClob")) {
      Class<?> jdbc = access.findClassByName("java.sql." + name);
      Class<?> wrapped = access.findClassByName(root + "Wrapped" + name);
      if (jdbc == null || wrapped == null) continue;
      RuntimeReflection.registerAllMethods(jdbc);
      RuntimeReflection.register(jdbc.getMethods());
      if (name.equals("NClob")) RuntimeProxyCreation.register(jdbc, wrapped);
      else RuntimeProxyCreation.register(jdbc, wrapped, Serializable.class);
    }
  }
}
