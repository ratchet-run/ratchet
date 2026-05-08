package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.Annotation;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.Recurring;

class RecurringAnnotationParserTest {

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
}
