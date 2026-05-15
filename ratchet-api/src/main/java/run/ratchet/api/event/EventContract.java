package run.ratchet.api.event;

import java.util.Objects;

final class EventContract {

  private EventContract() {}

  static <T> T requireNonNull(T value, String name) {
    return Objects.requireNonNull(value, name);
  }

  static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  static int requireNonNegative(int value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
    return value;
  }

  static Long requireNonNegative(Long value, String name) {
    if (value != null && value < 0) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
    return value;
  }

  static void requireBatchCounts(int totalItems, int completedItems, int failedItems) {
    requirePositive(totalItems, "totalItems");
    requireNonNegative(completedItems, "completedItems");
    requireNonNegative(failedItems, "failedItems");
    if ((long) completedItems + failedItems > totalItems) {
      throw new IllegalArgumentException("completedItems + failedItems must not exceed totalItems");
    }
  }
}
