package io.gambusia.mqtt;

import static io.gambusia.mqtt.MqttTopics.isValidFilter;
import static io.gambusia.mqtt.MqttTopics.isValidShareName;
import static io.gambusia.mqtt.MqttTopics.isValidTopic;
import static io.gambusia.mqtt.MqttTopics.matches;
import static io.gambusia.mqtt.MqttTopics.toSharedFilter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    assertTrue(isValidFilter("sport/tennis/#"));
    assertFalse(isValidFilter("sport/tennis#"));
    assertFalse(isValidFilter("sport/tennis/#/ranking"));
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

    assertTrue(isValidFilter("+"));
    assertTrue(isValidFilter("+/tennis/#"));
    assertFalse(isValidFilter("sport+"));
    assertTrue(isValidFilter("sport/+/player1"));

    assertTrue(matches("+/+", "/finance"));
    assertTrue(matches("/+", "/finance"));
    assertFalse(matches("+", "/finance"));
  }

  @Test
  void testTopic() {
    assertTrue(isValidTopic("sport/tennis/player1"));
    assertTrue(isValidTopic("sport/tennis/player1/ranking"));
    assertTrue(isValidTopic("sport/tennis/player1/score/wimbledon"));
    assertTrue(isValidTopic("sport"));
    assertTrue(isValidTopic("sport/"));
    assertTrue(isValidTopic("/finance"));

    assertFalse(isValidTopic(""));
    assertFalse(isValidTopic("\u0000"));
    assertFalse(isValidTopic("sport/tennis/#"));
    assertFalse(isValidTopic("+"));
    assertFalse(isValidTopic("+/tennis/#"));
    assertFalse(isValidTopic("sport/+/player1"));
  }

  @Test
  void testFilter() {
    assertTrue(isValidFilter("#"));

    assertFalse(isValidFilter(""));
    assertFalse(isValidFilter("\u0000"));
    assertFalse(isValidFilter("#?"));
    assertFalse(isValidFilter("+?"));

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
    assertTrue(isValidShareName("consumer1"));
    assertFalse(isValidShareName("consumer\u0000"));
    assertFalse(isValidShareName("consumer/"));
    assertFalse(isValidShareName("consumer#"));
    assertFalse(isValidShareName("consumer+"));

    assertEquals("$share/consumer1/+/tennis/#", toSharedFilter("consumer1", "+/tennis/#"));
  }
}
