/*
 * Copyright (C) 2018 Issey Yamakoshi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gambusia.netty.util;

public final class Args {
  private Args() {}

  public static int requirePositive(int i, String name) {
    if (i <= 0) {
      throw new IllegalArgumentException(name + ": " + i + " (expected: > 0)");
    }
    return i;
  }

  public static long requirePositive(long i, String name) {
    if (i <= 0) {
      throw new IllegalArgumentException(name + ": " + i + " (expected: > 0)");
    }
    return i;
  }

  public static int requireNonNegative(int i, String name) {
    if (i < 0) {
      throw new IllegalArgumentException(name + ": " + i + " (expected: >= 0)");
    }
    return i;
  }

  public static long requireNonNegative(long i, String name) {
    if (i < 0) {
      throw new IllegalArgumentException(name + ": " + i + " (expected: >= 0)");
    }
    return i;
  }

  public static int requireUnsignedShort(int i, String name) {
    if ((i & 0xFFFF0000) != 0) {
      throw new IllegalArgumentException(name + ": " + i + " (expected: 0–65535)");
    }
    return i;
  }

  public static long requireUnsignedInt(long i, String name) {
    if ((i & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException(name + ": " + i + " (expected: 0–4294967295)");
    }
    return i;
  }

  public static <T> T requireNonNull(T arg, String text) {
    if (arg == null) {
      throw new NullPointerException(text);
    }
    return arg;
  }

  public static String requireNonEmpty(String arg, String name) {
    if (requireNonNull(arg, name).isEmpty()) {
      throw new IllegalArgumentException(name + " is empty");
    }
    return arg;
  }

  public static <T> T[] requireNonEmpty(T[] array, String name) {
    if (requireNonNull(array, name).length <= 0) {
      throw new IllegalArgumentException(name + " is empty");
    }
    return array;
  }

  public static <T> T[] requireNonContainsNull(T[] array, String name) {
    requireNonEmpty(array, name);
    for (int i = 0; i < array.length; i++) {
      if (array[i] == null) {
        throw new NullPointerException(name + '[' + i + ']');
      }
    }
    return array;
  }
}
