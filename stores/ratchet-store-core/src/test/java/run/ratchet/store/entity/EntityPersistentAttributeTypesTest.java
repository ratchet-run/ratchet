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
package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embedded;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Transient;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.store.schema.RatchetJpaModel;

/**
 * Mirrors the Jakarta Persistence basic-type rule: @Convert only applies to basic attributes, and
 * strict providers drop attributes of other types unless explicitly mapped as relationships, etc.
 */
class EntityPersistentAttributeTypesTest {

  private static final List<Class<? extends Annotation>> NON_BASIC_MAPPINGS =
      List.of(
          ElementCollection.class,
          OneToMany.class,
          ManyToMany.class,
          ManyToOne.class,
          OneToOne.class,
          Embedded.class,
          EmbeddedId.class);

  @Test
  void persistentAttributesHaveBasicTypes() throws ClassNotFoundException {
    List<String> violations = new ArrayList<>();
    for (String className : RatchetJpaModel.ENTITY_CLASS_NAMES) {
      Class<?> entityType = Class.forName(className);
      for (Class<?> type = entityType; type != null; type = type.getSuperclass()) {
        if (type != entityType
            && !type.isAnnotationPresent(MappedSuperclass.class)
            && !type.isAnnotationPresent(Entity.class)) {
          continue;
        }
        for (Field field : type.getDeclaredFields()) {
          if (Modifier.isStatic(field.getModifiers())
              || Modifier.isTransient(field.getModifiers())
              || field.isAnnotationPresent(Transient.class)
              || NON_BASIC_MAPPINGS.stream().anyMatch(field::isAnnotationPresent)) {
            continue;
          }
          if (!isBasicType(field.getType())) {
            violations.add(
                type.getSimpleName()
                    + "."
                    + field.getName()
                    + " ("
                    + field.getType().getTypeName()
                    + ")");
          }
        }
      }
    }
    assertTrue(
        violations.isEmpty(),
        () -> "Persistent attributes with non-basic types:\n" + String.join("\n", violations));
  }

  private static boolean isBasicType(Class<?> type) {
    // All standard basic reference types (wrappers, String, BigInteger/BigDecimal, util/sql dates,
    // Calendar, java.time types, UUID and byte/character arrays) implement Serializable.
    return type.isPrimitive() || type.isEnum() || Serializable.class.isAssignableFrom(type);
  }
}
