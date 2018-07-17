package io.gambusia.mqtt;

import static io.gambusia.mqtt.MqttTopics.matches;
import static io.gambusia.mqtt.MqttTopics.toSharedFilter;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MqttTopicsTest {

  @Test
  void testMultiLevelWildcard() {
    // http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
    // 4.7.1.2 Multi-level wildcard
    assertTrue(matches("sport/tennis/player1/#", "sport/tennis/player1"));
    assertTrue(matches("sport/tennis/player1/#", "sport/tennis/player1/ranking"));
    assertTrue(matches("sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon"));

    assertChecker(MqttTopics::checkFilter, "sport/tennis/#", "filter");
    assertIllegalArgumentException(MqttTopics::checkFilter, "sport/tennis#", "filter");
    assertIllegalArgumentException(MqttTopics::checkFilter, "sport/tennis/#/ranking", "filter");
  }

  @Test
  void testSingleLevelWildcard() {
    // http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
    // 4.7.1.3 Single level wildcard
    assertTrue(matches("sport/tennis/+", "sport/tennis/player1"));
    assertTrue(matches("sport/tennis/+", "sport/tennis/player2"));
    assertFalse(matches("sport/tennis/+", "sport/tennis/player1/ranking"));

    assertFalse(matches("sport/+", "sport"));
    assertFalse(matches("sport/+", "sport/"));

    assertChecker(MqttTopics::checkFilter, "+", "filter");
    assertChecker(MqttTopics::checkFilter, "+/tennis/#", "filter");
    assertIllegalArgumentException(MqttTopics::checkFilter, "sport+", "filter");
    assertChecker(MqttTopics::checkFilter, "sport/+/player1", "filter");

    assertTrue(matches("+/+", "/finance"));
    assertTrue(matches("/+", "/finance"));
    assertFalse(matches("+", "/finance"));
  }

  @Test
  void testTopic() {
    assertChecker(MqttTopics::checkTopic, "sport/tennis/player1", "topic");
    assertChecker(MqttTopics::checkTopic, "sport/tennis/player1/ranking", "topic");
    assertChecker(MqttTopics::checkTopic, "sport/tennis/player1/score/wimbledon", "topic");
    assertChecker(MqttTopics::checkTopic, "sport", "topic");
    assertChecker(MqttTopics::checkTopic, "sport/", "topic");
    assertChecker(MqttTopics::checkTopic, "/finance", "topic");

    assertNullPointerException(MqttTopics::checkTopic, (CharSequence) null, "topic");
    assertIllegalArgumentException(MqttTopics::checkTopic, "", "topic");
    assertIllegalArgumentException(MqttTopics::checkTopic, "\u0000", "topic");
    assertIllegalArgumentException(MqttTopics::checkTopic, "sport/tennis/#", "topic");
    assertIllegalArgumentException(MqttTopics::checkTopic, "+", "topic");
    assertIllegalArgumentException(MqttTopics::checkTopic, "+/tennis/#", "topic");
    assertIllegalArgumentException(MqttTopics::checkTopic, "sport/+/player1", "topic");
  }

  @Test
  void testFilter() {
    assertChecker(MqttTopics::checkFilter, "#", "filter");

    assertNullPointerException(MqttTopics::checkFilter, (CharSequence) null, "filter");
    assertIllegalArgumentException(MqttTopics::checkFilter, "", "filter");
    assertIllegalArgumentException(MqttTopics::checkFilter, "\u0000", "filter");
    assertIllegalArgumentException(MqttTopics::checkFilter, "#?", "filter");
    assertIllegalArgumentException(MqttTopics::checkFilter, "+?", "filter");

    assertTrue(matches("sport/tennis/player1/#", "sport/tennis/player1/"));
    assertFalse(matches("sport/football/player1/#", "sport/tennis/player1"));
    assertFalse(matches("sport/football/+", "sport/tennis/player1"));

    assertTrue(matches("sport/tennis/+/#", "sport/tennis/"));
    assertFalse(matches("sport/tennis/+/#", "sport/tennis"));
    assertFalse(matches("sport/tennis/?/#", "sport/tennis/"));
    assertFalse(matches("sport/tennis/+/?", "sport/tennis/"));
  }

  @Test
  void testSharedFilter() {
    assertChecker(MqttTopics::checkShareName, "consumer1", "shareName");
    assertNullPointerException(MqttTopics::checkShareName, (CharSequence) null, "shareName");
    assertIllegalArgumentException(MqttTopics::checkShareName, "consumer\u0000", "shareName");
    assertIllegalArgumentException(MqttTopics::checkShareName, "consumer/", "shareName");
    assertIllegalArgumentException(MqttTopics::checkShareName, "consumer#", "shareName");
    assertIllegalArgumentException(MqttTopics::checkShareName, "consumer+", "shareName");

    assertEquals("$share/consumer1/+/tennis/#", toSharedFilter("consumer1", "+/tennis/#"));
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
}
