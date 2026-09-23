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
package run.ratchet.spring.boot.autoconfigure.internal;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.aot.generate.GeneratedFiles;
import org.springframework.core.io.ByteArrayResource;

/** Shared build resources may be contributed by several Spring test contexts. */
public final class AotResources {
  private AotResources() {}

  public static String contentKey(String content) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(content.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException failure) {
      throw new IllegalStateException("SHA-256 is unavailable", failure);
    }
  }

  public static void add(GeneratedFiles files, String path, String content) {
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    files.handleFile(
        GeneratedFiles.Kind.RESOURCE,
        path,
        handler -> {
          if (!handler.exists()) {
            handler.create(new ByteArrayResource(bytes));
            return;
          }
          try (var existing =
              java.util.Objects.requireNonNull(handler.getContent()).getInputStream()) {
            if (!java.util.Arrays.equals(existing.readAllBytes(), bytes)) {
              throw new IllegalStateException(
                  "Incompatible Ratchet AOT contexts generated " + path);
            }
          }
        });
  }
}
