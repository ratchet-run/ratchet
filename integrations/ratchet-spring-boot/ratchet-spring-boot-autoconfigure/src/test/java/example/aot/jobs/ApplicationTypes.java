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
package example.aot.jobs;

import java.io.Serializable;

public class ApplicationTypes {
  static {
    if (Boolean.parseBoolean("true")) throw new AssertionError("AOT initialized a submitter");
  }

  public static class Nested {
    public void execute(Payload payload) {}
  }

  public record Payload(String value) {}

  public static class Pojo {
    public String value;

    public Pojo() {}
  }

  public static Class<?>[] lexicalTypes() {
    class Local implements Serializable {}
    return new Class<?>[] {
      Local.class,
      new Serializable() {
        public Serializable lambda() {
          return (Runnable & Serializable) () -> {};
        }
      }.getClass()
    };
  }
}
