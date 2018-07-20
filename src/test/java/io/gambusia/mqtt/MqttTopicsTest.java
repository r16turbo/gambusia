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

package io.gambusia.mqtt;

import static io.gambusia.mqtt.MqttTopics.matches;
import static io.gambusia.mqtt.MqttTopics.requireValidFilter;
import static io.gambusia.mqtt.MqttTopics.requireValidShareName;
import static io.gambusia.mqtt.MqttTopics.requireValidTopic;
import static io.gambusia.mqtt.MqttTopics.toSharedFilter;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MqttTopicsTest {

  static final CharSequence overflow = new DummyCharSequence(MqttTopics.MAX_CHARS + 1);
  static final Checker<CharSequence> topicChecker = MqttTopics::requireValidTopic;
  static final Checker<CharSequence> filterChecker = MqttTopics::requireValidFilter;
  static final Checker<CharSequence> shareNameChecker = MqttTopics::requireValidShareName;

  @Test
  void testMultiLevelWildcard() {
    // http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
    // 4.7.1.2 Multi-level wildcard
    assertMatches(true, "sport/tennis/player1/#", "sport/tennis/player1");
    assertMatches(true, "sport/tennis/player1/#", "sport/tennis/player1/ranking");
    assertMatches(true, "sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon");

    assertChecker(filterChecker, "sport/tennis/#", "filter");
    assertIllegalArgumentException(filterChecker, "sport/tennis#", "filter");
    assertIllegalArgumentException(filterChecker, "sport/tennis/#/ranking", "filter");
  }

  @Test
  void testSingleLevelWildcard() {
    // http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
    // 4.7.1.3 Single level wildcard
    assertMatches(true, "sport/tennis/+", "sport/tennis/player1");
    assertMatches(true, "sport/tennis/+", "sport/tennis/player2");
    assertMatches(false, "sport/tennis/+", "sport/tennis/player1/ranking");

    assertMatches(false, "sport/+", "sport");
    assertMatches(false, "sport/+", "sport/");

    assertChecker(filterChecker, "+", "filter");
    assertChecker(filterChecker, "+/tennis/#", "filter");
    assertIllegalArgumentException(filterChecker, "sport+", "filter");
    assertChecker(filterChecker, "sport/+/player1", "filter");

    assertMatches(true, "+/+", "/finance");
    assertMatches(true, "/+", "/finance");
    assertMatches(false, "+", "/finance");
  }

  @Test
  void testTopic() {
    assertChecker(topicChecker, "sport/tennis/player1", "topic");
    assertChecker(topicChecker, "sport/tennis/player1/ranking", "topic");
    assertChecker(topicChecker, "sport/tennis/player1/score/wimbledon", "topic");
    assertChecker(topicChecker, "sport", "topic");
    assertChecker(topicChecker, "sport/", "topic");
    assertChecker(topicChecker, "/finance", "topic");

    assertNullPointerException(topicChecker, (CharSequence) null, "topic");
    assertIllegalArgumentException(topicChecker, "", "topic");
    assertIllegalArgumentException(topicChecker, overflow, "topic");
    assertIllegalArgumentException(topicChecker, "\u0000", "topic");
    assertIllegalArgumentException(topicChecker, "sport/tennis/#", "topic");
    assertIllegalArgumentException(topicChecker, "+", "topic");
    assertIllegalArgumentException(topicChecker, "+/tennis/#", "topic");
    assertIllegalArgumentException(topicChecker, "sport/+/player1", "topic");
  }

  @Test
  void testFilter() {
    assertChecker(filterChecker, "#", "filter");

    assertNullPointerException(filterChecker, (CharSequence) null, "filter");
    assertIllegalArgumentException(filterChecker, "", "filter");
    assertIllegalArgumentException(filterChecker, overflow, "filter");
    assertIllegalArgumentException(filterChecker, "\u0000", "filter");
    assertIllegalArgumentException(filterChecker, "#?", "filter");
    assertIllegalArgumentException(filterChecker, "+?", "filter");

    assertMatches(true, "sport/tennis/player1/#", "sport/tennis/player1/");
    assertMatches(false, "sport/football/player1/#", "sport/tennis/player1");
    assertMatches(false, "sport/football/+", "sport/tennis/player1");

    assertMatches(true, "sport/tennis/+/#", "sport/tennis/");
    assertMatches(false, "sport/tennis/+/#", "sport/tennis");
    assertMatches(false, "sport/tennis/?/#", "sport/tennis/");
    assertMatches(false, "sport/tennis/+/?", "sport/tennis/");

    assertMatches(true, "sport/tennis/+/", "sport/tennis//");
    assertMatches(false, "sport/tennis/+", "sport/tennis//");
  }

  @Test
  void testSharedFilter() {
    assertChecker(shareNameChecker, "consumer1", "shareName");
    assertNullPointerException(shareNameChecker, (CharSequence) null, "shareName");
    assertIllegalArgumentException(shareNameChecker, "", "shareName");
    assertIllegalArgumentException(shareNameChecker, overflow, "shareName");
    assertIllegalArgumentException(shareNameChecker, "consumer\u0000", "shareName");
    assertIllegalArgumentException(shareNameChecker, "consumer/", "shareName");
    assertIllegalArgumentException(shareNameChecker, "consumer#", "shareName");
    assertIllegalArgumentException(shareNameChecker, "consumer+", "shareName");

    assertSharedFilter("$share/consumer1/+/tennis/#", "consumer1", "+/tennis/#");
  }

  interface Checker<V> {
    V check(V value, String name) throws Exception;
  }

  static <V> void assertChecker(Checker<V> checker, V value, String name) {
    assertDoesNotThrow(() -> assertEquals(value, checker.check(value, name)));
  }

  static <V> void assertNullPointerException(Checker<V> checker, V value, String name) {
    assertEquals(name, assertThrows(NullPointerException.class, () -> {
      checker.check(value, name);
    }).getMessage());
  }

  static <V> void assertIllegalArgumentException(Checker<V> checker, V value, String name) {
    assertEquals(name, assertThrows(IllegalArgumentException.class, () -> {
      checker.check(value, name);
    }).getMessage());
  }

  static void assertMatches(boolean match, CharSequence filter, CharSequence topic) {
    if (match) {
      assertTrue(matches(requireValidFilter(filter, "filter"), requireValidTopic(topic, "topic")));
    } else {
      assertFalse(matches(requireValidFilter(filter, "filter"), requireValidTopic(topic, "topic")));
    }
  }

  static void assertSharedFilter(String sharedFilter, CharSequence shareName, CharSequence filter) {
    requireValidShareName(shareName, "shareName");
    requireValidFilter(filter, "filter");
    assertEquals(sharedFilter, toSharedFilter(shareName, filter));
  }

  static class DummyCharSequence implements CharSequence {

    private final int length;

    public DummyCharSequence(int length) {
      this.length = length;
    }

    @Override
    public int length() {
      return length;
    }

    @Override
    public char charAt(int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      throw new UnsupportedOperationException();
    }
  }
}
