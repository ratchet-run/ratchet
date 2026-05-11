package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class JobPageTest {

  @Test
  void constructorDefensivelyCopiesItems() {
    List<String> items = new ArrayList<>(List.of("one"));
    JobPage<String> page = new JobPage<>(items, 1, 10, 0, false, null);

    items.add("two");

    assertEquals(List.of("one"), page.items());
    assertThrows(UnsupportedOperationException.class, () -> page.items().add("three"));
  }
}
