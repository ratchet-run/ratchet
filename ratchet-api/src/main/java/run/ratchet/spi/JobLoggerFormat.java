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

/**
 * Minimal SLF4J-style {@code {}} placeholder substitution for {@link JobLogger} default methods.
 * Does not depend on SLF4J so {@code ratchet-api} stays free of logging-facade dependencies.
 */
final class JobLoggerFormat {

  private JobLoggerFormat() {}

  static String format(String format, Object... args) {
    if (format == null) {
      return "null";
    }
    if (args == null || args.length == 0) {
      return format;
    }
    StringBuilder sb = new StringBuilder(format.length() + 16 * args.length);
    int argIdx = 0;
    int i = 0;
    while (i < format.length()) {
      if (i + 1 < format.length() && format.charAt(i) == '{' && format.charAt(i + 1) == '}') {
        if (argIdx < args.length) {
          sb.append(args[argIdx++]);
        } else {
          sb.append("{}");
        }
        i += 2;
      } else {
        sb.append(format.charAt(i));
        i++;
      }
    }
    if (argIdx < args.length) {
      sb.append(" [extra args:");
      for (int j = argIdx; j < args.length; j++) {
        sb.append(' ').append(args[j]);
        if (j < args.length - 1) {
          sb.append(',');
        }
      }
      sb.append(']');
    }
    return sb.toString();
  }
}
