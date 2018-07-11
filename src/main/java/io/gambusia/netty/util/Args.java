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

public abstract class Args {

  public static <T> T checkNotNull(T arg, String text) {
    return io.netty.util.internal.ObjectUtil.checkNotNull(arg, text);
  }

  public static int checkPositive(int i, String name) {
    return io.netty.util.internal.ObjectUtil.checkPositive(i, name);
  }

  public static long checkPositive(long i, String name) {
    return io.netty.util.internal.ObjectUtil.checkPositive(i, name);
  }

  public static int checkPositiveOrZero(int i, String name) {
    return io.netty.util.internal.ObjectUtil.checkPositiveOrZero(i, name);
  }

  public static long checkPositiveOrZero(long i, String name) {
    return io.netty.util.internal.ObjectUtil.checkPositiveOrZero(i, name);
  }

  public static <T> T[] checkNonEmpty(T[] array, String name) {
    return io.netty.util.internal.ObjectUtil.checkNonEmpty(array, name);
  }

  public static int intValue(Integer wrapper, int defaultValue) {
    return io.netty.util.internal.ObjectUtil.intValue(wrapper, defaultValue);
  }

  public static long longValue(Long wrapper, long defaultValue) {
    return io.netty.util.internal.ObjectUtil.longValue(wrapper, defaultValue);
  }

  public static String checkNotEmpty(String arg, String text) {
    io.netty.util.internal.ObjectUtil.checkNotNull(arg, text);
    if (arg.isEmpty()) {
      throw new IllegalArgumentException(text);
    }
    return arg;
  }

  public static <T> T[] checkNotContainsNull(T[] array, String name) {
    checkNonEmpty(array, name);
    for (int i = 0; i < array.length; i++) {
      if (array[i] == null) {
        throw new NullPointerException(name + "[" + i + "]");
      }
    }
    return array;
  }
}
