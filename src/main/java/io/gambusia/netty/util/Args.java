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

  public static int checkPositive(int i, String name) {
    if (i <= 0) {
      throw new IllegalArgumentException(String.format("%s: %d (expected: > 0)", name, i));
    }
    return i;
  }

  public static long checkPositive(long i, String name) {
    if (i <= 0) {
      throw new IllegalArgumentException(String.format("%s: %d (expected: > 0)", name, i));
    }
    return i;
  }

  public static int checkPositiveOrZero(int i, String name) {
    if (i < 0) {
      throw new IllegalArgumentException(String.format("%s: %d (expected: >= 0)", name, i));
    }
    return i;
  }

  public static long checkPositiveOrZero(long i, String name) {
    if (i < 0) {
      throw new IllegalArgumentException(String.format("%s: %d (expected: >= 0)", name, i));
    }
    return i;
  }

  public static int checkUnsignedShort(int i, String name) {
    if ((i & 0xFFFF0000) != 0) {
      throw new IllegalArgumentException(String.format("%s: %d (expected: 0–65535)", name, i));
    }
    return i;
  }

  public static long checkUnsignedInt(long i, String name) {
    if ((i & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException(String.format("%s: %d (expected: 0–4294967295)", name, i));
    }
    return i;
  }

  public static <T> T checkNotNull(T arg, String text) {
    if (arg == null) {
      throw new NullPointerException(text);
    }
    return arg;
  }

  public static String checkNotEmpty(String arg, String name) {
    if (checkNotNull(arg, name).isEmpty()) {
      throw new IllegalArgumentException(String.format("%s is empty", name));
    }
    return arg;
  }

  public static <T> T[] checkNotEmpty(T[] array, String name) {
    if (checkNotNull(array, name).length <= 0) {
      throw new IllegalArgumentException(String.format("%s is empty", name));
    }
    return array;
  }

  public static <T> T[] checkNotContainsNull(T[] array, String name) {
    checkNotEmpty(array, name);
    for (int i = 0; i < array.length; i++) {
      if (array[i] == null) {
        throw new NullPointerException(String.format("%s[%d]", name, i));
      }
    }
    return array;
  }
}
