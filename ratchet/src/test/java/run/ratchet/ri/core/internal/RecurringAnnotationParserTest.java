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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.Annotation;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringMisfirePolicy;

class RecurringAnnotationParserTest {

  private static final class MisfireFixtures {

    @Recurring(cron = "0 * * * * ?", misfirePolicy = RecurringMisfirePolicy.Action.SKIP)
    public void skip() {}

    @Recurring(cron = "0 * * * * ?", misfirePolicy = RecurringMisfirePolicy.Action.FIRE_ONCE)
    public void fireOnce() {}

    @Recurring(
        cron = "0 * * * * ?",
        misfirePolicy = RecurringMisfirePolicy.Action.CATCH_UP,
        maxCatchUpExecutions = 4)
    public void catchUp() {}

    @Recurring(
        cron = "0 * * * * ?",
        misfirePolicy = RecurringMisfirePolicy.Action.CATCH_UP,
        maxCatchUpExecutions = 0)
    public void invalidCatchUp() {}
  }

  private static Recurring recurring(String id, String cron) {
    return new Recurring() {
      @Override
      public Class<? extends Annotation> annotationType() {
        return Recurring.class;
      }

      @Override
      public String id() {
        return id;
      }

      @Override
      public String cron() {
        return cron;
      }

      @Override
      public String zone() {
        return "UTC";
      }

      @Override
      public String name() {
        return "";
      }

      @Override
      public boolean enabled() {
        return true;
      }

      @Override
      public int priority() {
        return 5;
      }

      @Override
      public int maxRetries() {
        return 3;
      }

      @Override
      public RecurringMisfirePolicy.Action misfirePolicy() {
        return RecurringMisfirePolicy.Action.CATCH_UP;
      }

      @Override
      public int maxCatchUpExecutions() {
        return RecurringMisfirePolicy.DEFAULT_MAX_CATCH_UP_EXECUTIONS;
      }

      @Override
      public BackoffPolicy backoffPolicy() {
        return BackoffPolicy.EXPONENTIAL;
      }

      @Override
      public long backoffDelayMs() {
        return 1000;
      }

      @Override
      public long timeoutSeconds() {
        return 3600;
      }

      @Override
      public String[] tags() {
        return new String[0];
      }
    };
  }

  private static Recurring recurringWithEnabled(boolean enabled) {
    return new Recurring() {
      @Override
      public Class<? extends Annotation> annotationType() {
        return Recurring.class;
      }

      @Override
      public String id() {
        return "";
      }

      @Override
      public String cron() {
        return "0 * * * * ?";
      }

      @Override
      public String zone() {
        return "UTC";
      }

      @Override
      public String name() {
        return "";
      }

      @Override
      public boolean enabled() {
        return enabled;
      }

      @Override
      public int priority() {
        return 5;
      }

      @Override
      public int maxRetries() {
        return 3;
      }

      @Override
      public RecurringMisfirePolicy.Action misfirePolicy() {
        return RecurringMisfirePolicy.Action.CATCH_UP;
      }

      @Override
      public int maxCatchUpExecutions() {
        return RecurringMisfirePolicy.DEFAULT_MAX_CATCH_UP_EXECUTIONS;
      }

      @Override
      public BackoffPolicy backoffPolicy() {
        return BackoffPolicy.EXPONENTIAL;
      }

      @Override
      public long backoffDelayMs() {
        return 1000;
      }

      @Override
      public long timeoutSeconds() {
        return 3600;
      }

      @Override
      public String[] tags() {
        return new String[0];
      }
    };
  }

  @Test
  void generateJobId_usesExplicitId() {
    Recurring annotation = recurring("my-custom-id", "0 * * * * ?");
    assertEquals(
        "my-custom-id",
        RecurringAnnotationParser.generateJobId(annotation, "com.example.MyService", "myMethod"));
  }

  @Test
  void generateJobId_generatesFromClassAndMethod() {
    Recurring annotation = recurring("", "0 * * * * ?");
    assertEquals(
        "com.example.MyService.myMethod",
        RecurringAnnotationParser.generateJobId(annotation, "com.example.MyService", "myMethod"));
  }

  @Test
  void generateJobId_rejectsExplicitIdOutsideThePortableContract() {
    Recurring annotation = recurring("récurrent", "0 * * * * ?");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            RecurringAnnotationParser.generateJobId(
                annotation, "com.example.MyService", "myMethod"));
  }

  @Test
  void generateJobId_rejectsOverlongDerivedId() {
    Recurring annotation = recurring("", "0 * * * * ?");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            RecurringAnnotationParser.generateJobId(
                annotation, "com.example." + "a".repeat(240), "myMethod"));
  }

  @Test
  void isEnabled_defaultTrue() {
    Recurring annotation = recurring("", "0 * * * * ?");
    assertTrue(RecurringAnnotationParser.isEnabled(annotation));
  }

  @Test
  void isEnabled_explicitFalse() {
    Recurring annotation = recurringWithEnabled(false);
    assertFalse(RecurringAnnotationParser.isEnabled(annotation));
  }

  @Test
  void mapPriority_lowestRange() {
    assertEquals(JobPriority.LOWEST, RecurringAnnotationParser.mapPriority(1));
    assertEquals(JobPriority.LOWEST, RecurringAnnotationParser.mapPriority(2));
  }

  @Test
  void mapPriority_lowRange() {
    assertEquals(JobPriority.LOW, RecurringAnnotationParser.mapPriority(3));
    assertEquals(JobPriority.LOW, RecurringAnnotationParser.mapPriority(4));
  }

  @Test
  void mapPriority_normalRange() {
    assertEquals(JobPriority.NORMAL, RecurringAnnotationParser.mapPriority(5));
    assertEquals(JobPriority.NORMAL, RecurringAnnotationParser.mapPriority(6));
  }

  @Test
  void mapPriority_highRange() {
    assertEquals(JobPriority.HIGH, RecurringAnnotationParser.mapPriority(7));
    assertEquals(JobPriority.HIGH, RecurringAnnotationParser.mapPriority(8));
  }

  @Test
  void mapPriority_criticalRange() {
    assertEquals(JobPriority.CRITICAL, RecurringAnnotationParser.mapPriority(9));
    assertEquals(JobPriority.CRITICAL, RecurringAnnotationParser.mapPriority(10));
  }

  @Test
  void mapPriority_rejectsValuesBelowDocumentedRange() {
    assertThrows(IllegalArgumentException.class, () -> RecurringAnnotationParser.mapPriority(0));
    assertThrows(IllegalArgumentException.class, () -> RecurringAnnotationParser.mapPriority(-5));
  }

  @Test
  void mapPriority_rejectsValuesAboveDocumentedRange() {
    assertThrows(IllegalArgumentException.class, () -> RecurringAnnotationParser.mapPriority(11));
    assertThrows(IllegalArgumentException.class, () -> RecurringAnnotationParser.mapPriority(100));
  }

  @Test
  void misfirePolicyMapsEveryAnnotationAction() throws NoSuchMethodException {
    assertEquals(
        RecurringMisfirePolicy.skip(),
        RecurringAnnotationParser.misfirePolicy(annotationOn("skip")));
    assertEquals(
        RecurringMisfirePolicy.fireOnce(),
        RecurringAnnotationParser.misfirePolicy(annotationOn("fireOnce")));
    assertEquals(
        RecurringMisfirePolicy.catchUp(4),
        RecurringAnnotationParser.misfirePolicy(annotationOn("catchUp")));
  }

  @Test
  void misfirePolicyRejectsInvalidCatchUpLimit() throws NoSuchMethodException {
    assertThrows(
        IllegalArgumentException.class,
        () -> RecurringAnnotationParser.misfirePolicy(annotationOn("invalidCatchUp")));
  }

  private static Recurring annotationOn(String methodName) throws NoSuchMethodException {
    return MisfireFixtures.class.getMethod(methodName).getAnnotation(Recurring.class);
  }
}
