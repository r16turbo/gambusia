/*
 * Copyright 2018 Issey Yamakoshi
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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("args")
class ArgsTest {
  @Test
  void requirePositiveInteger() {
    assertDoesNotThrow(() -> {
      assertEquals(Integer.MAX_VALUE, Args.requirePositive(Integer.MAX_VALUE, "arg"));
    });
    assertDoesNotThrow(() -> {
      assertEquals(1, Args.requirePositive(1, "arg"));
    });
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requirePositive(0, "arg"))
        .withNoCause().withMessage("arg: %d (expected: > 0)", 0);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requirePositive(Integer.MIN_VALUE, "arg"))
        .withNoCause().withMessage("arg: %d (expected: > 0)", Integer.MIN_VALUE);
  }

  @Test
  void requirePositiveLong() {
    assertDoesNotThrow(() -> {
      assertEquals(Long.MAX_VALUE, Args.requirePositive(Long.MAX_VALUE, "arg"));
    });
    assertDoesNotThrow(() -> {
      assertEquals(1L, Args.requirePositive(1L, "arg"));
    });

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requirePositive(0L, "arg"))
        .withNoCause().withMessage("arg: %d (expected: > 0)", 0);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requirePositive(Long.MIN_VALUE, "arg"))
        .withNoCause().withMessage("arg: %d (expected: > 0)", Long.MIN_VALUE);
  }

  @Test
  void requireNonNegativeInteger() {
    assertDoesNotThrow(() -> {
      assertEquals(Integer.MAX_VALUE, Args.requireNonNegative(Integer.MAX_VALUE, "arg"));
    });
    assertDoesNotThrow(() -> {
      assertEquals(0, Args.requireNonNegative(0, "arg"));
    });

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireNonNegative(-1, "arg"))
        .withNoCause().withMessage("arg: %d (expected: >= 0)", -1);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireNonNegative(Integer.MIN_VALUE, "arg"))
        .withNoCause().withMessage("arg: %d (expected: >= 0)", Integer.MIN_VALUE);
  }

  @Test
  void requireNonNegativeLong() {
    assertDoesNotThrow(() -> {
      assertEquals(Long.MAX_VALUE, Args.requireNonNegative(Long.MAX_VALUE, "arg"));
    });
    assertDoesNotThrow(() -> {
      assertEquals(0L, Args.requireNonNegative(0L, "arg"));
    });

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireNonNegative(-1L, "arg"))
        .withNoCause().withMessage("arg: %d (expected: >= 0)", -1L);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireNonNegative(Long.MIN_VALUE, "arg"))
        .withNoCause().withMessage("arg: %d (expected: >= 0)", Long.MIN_VALUE);
  }

  @Test
  void requireUnsignedShort() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireUnsignedShort(Integer.MAX_VALUE, "arg"))
        .withNoCause().withMessage("arg: %d (expected: 0–65535)", Integer.MAX_VALUE);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireUnsignedShort(65536, "arg"))
        .withNoCause().withMessage("arg: %d (expected: 0–65535)", 65536);

    assertDoesNotThrow(() -> {
      assertEquals(65535, Args.requireUnsignedShort(65535, "arg"));
    });
    assertDoesNotThrow(() -> {
      assertEquals(0, Args.requireUnsignedShort(0, "arg"));
    });

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireUnsignedShort(-1, "arg"))
        .withNoCause().withMessage("arg: %d (expected: 0–65535)", -1);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireUnsignedShort(Integer.MIN_VALUE, "arg"))
        .withNoCause().withMessage("arg: %d (expected: 0–65535)", Integer.MIN_VALUE);
  }

  @Test
  void requireUnsignedInt() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireUnsignedInt(Long.MAX_VALUE, "arg"))
        .withNoCause().withMessage("arg: %d (expected: 0–4294967295)", Long.MAX_VALUE);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireUnsignedInt(4294967296L, "arg"))
        .withNoCause().withMessage("arg: %d (expected: 0–4294967295)", 4294967296L);

    assertDoesNotThrow(() -> {
      assertEquals(4294967295L, Args.requireUnsignedInt(4294967295L, "arg"));
    });
    assertDoesNotThrow(() -> {
      assertEquals(0L, Args.requireUnsignedInt(0L, "arg"));
    });

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireUnsignedInt(-1L, "arg"))
        .withNoCause().withMessage("arg: %d (expected: 0–4294967295)", -1L);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireUnsignedInt(Long.MIN_VALUE, "arg"))
        .withNoCause().withMessage("arg: %d (expected: 0–4294967295)", Long.MIN_VALUE);
  }

  @Test
  void requireNonNull() {
    assertDoesNotThrow(() -> {
      Object arg = new Object();
      assertEquals(arg, Args.requireNonNull(arg, "arg"));
    });

    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> Args.requireNonNull(null, "arg"))
        .withNoCause().withMessage("arg");
  }

  @Test
  void requireNonEmptyString() {
    assertDoesNotThrow(() -> {
      String arg = "test";
      assertEquals(arg, Args.requireNonEmpty(arg, "arg"));
    });

    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> Args.requireNonEmpty((String) null, "arg"))
        .withNoCause().withMessage("arg");
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireNonEmpty("", "arg"))
        .withNoCause().withMessage("arg is empty");
  }

  @Test
  void requireNonEmptyArray() {
    assertDoesNotThrow(() -> {
      Object[] array = new Object[1];
      assertArrayEquals(array, Args.requireNonEmpty(array, "arg"));
    });

    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> Args.requireNonEmpty((Object[]) null, "arg"))
        .withNoCause().withMessage("arg");
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireNonEmpty(new Object[0], "arg"))
        .withNoCause().withMessage("arg is empty");
  }

  @Test
  void requireNonContainsNull() {
    assertDoesNotThrow(() -> {
      Object[] array = new Object[] {new Object()};
      assertArrayEquals(array, Args.requireNonContainsNull(array, "arg"));
    });

    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> Args.requireNonContainsNull((Object[]) null, "arg"))
        .withNoCause().withMessage("arg");
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Args.requireNonContainsNull(new Object[0], "arg"))
        .withNoCause().withMessage("arg is empty");

    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> {
          Args.requireNonContainsNull(new Object[] {null, new Object(), new Object()}, "arg");
        })
        .withNoCause().withMessage("arg[0]");
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> {
          Args.requireNonContainsNull(new Object[] {new Object(), null, new Object()}, "arg");
        })
        .withNoCause().withMessage("arg[1]");
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> {
          Args.requireNonContainsNull(new Object[] {new Object(), new Object(), null}, "arg");
        })
        .withNoCause().withMessage("arg[2]");
  }
}
