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
package run.ratchet.spi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class EncryptionContextTest {

  private static final EncryptionKey KEY = () -> "key-1";

  @Test
  void accessors_returnConstructorArguments() {
    UUID jobId = UUID.randomUUID();
    EncryptionContext ctx =
        new EncryptionContext(ProtectedSurface.PAYLOAD_ARGS, jobId, KEY, new byte[] {1, 2, 3});

    assertEquals(ProtectedSurface.PAYLOAD_ARGS, ctx.surface());
    assertEquals(jobId, ctx.jobId());
    assertSame(KEY, ctx.key());
    assertArrayEquals(new byte[] {1, 2, 3}, ctx.additionalAuthenticatedData());
  }

  @Test
  void nullJobId_isAllowedForSurfaceOnlyBinding() {
    EncryptionContext ctx =
        new EncryptionContext(ProtectedSurface.SIGNAL_PAYLOAD, null, KEY, new byte[] {9});

    assertNull(ctx.jobId());
  }

  @Test
  void constructor_copiesAad_soLaterCallerMutationDoesNotLeakIn() {
    byte[] source = {1, 2, 3};
    EncryptionContext ctx =
        new EncryptionContext(ProtectedSurface.RESULT, null, KEY, source);

    source[0] = 99; // mutate the caller's array after construction

    assertArrayEquals(new byte[] {1, 2, 3}, ctx.additionalAuthenticatedData());
  }

  @Test
  void accessor_returnsCopy_soCallerMutationDoesNotLeakOut() {
    EncryptionContext ctx =
        new EncryptionContext(ProtectedSurface.RESULT, null, KEY, new byte[] {1, 2, 3});

    byte[] first = ctx.additionalAuthenticatedData();
    first[0] = 99; // mutate the array the accessor returned
    byte[] second = ctx.additionalAuthenticatedData();

    assertArrayEquals(new byte[] {1, 2, 3}, second);
    assertNotSame(first, second);
  }

  @Test
  void nullSurface_keyOrAad_areRejected() {
    assertThrows(
        NullPointerException.class,
        () -> new EncryptionContext(null, null, KEY, new byte[0]));
    assertThrows(
        NullPointerException.class,
        () -> new EncryptionContext(ProtectedSurface.RESULT, null, null, new byte[0]));
    assertThrows(
        NullPointerException.class,
        () -> new EncryptionContext(ProtectedSurface.RESULT, null, KEY, null));
  }
}
