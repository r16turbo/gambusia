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

import java.nio.charset.StandardCharsets;

public final class MqttTopics {
  private MqttTopics() {}

  public static final int MIN_BYTES = 1;
  public static final int MAX_BYTES = 65535;

  public static final char NUL = '\u0000';

  public static boolean isValidLength(CharSequence topic) {
    final int length = topic.toString().getBytes(StandardCharsets.UTF_8).length;
    if (length < MIN_BYTES || length > MAX_BYTES) {
      return false;
    } else {
      return true;
    }
  }

  public static boolean isValidTopic(CharSequence topic) {
    if (!isValidLength(topic)) {
      return false;
    }

    final int length = topic.length();
    for (int index = 0; index < length; index++) {
      final char current = topic.charAt(index);
      if (current == NUL || current == '#' || current == '+') {
        return false;
      }
    }

    return true;
  }

  public static boolean isValidFilter(CharSequence filter) {
    if (!isValidLength(filter)) {
      return false;
    }

    final int length = filter.length();
    for (int index = 0; index < length; index++) {
      final char current = filter.charAt(index);
      if (current == NUL) {
        return false;
      } else if (current == '#') {
        if ((index <= 0 || filter.charAt(index - 1) == '/')
            && (index + 1 >= length)) {
          // valid: (previous is none or '/') and (next is none)
        } else {
          return false;
        }
      } else if (current == '+') {
        if ((index <= 0 || filter.charAt(index - 1) == '/')
            && (index + 1 >= length || filter.charAt(index + 1) == '/')) {
          // valid: (previous is none or '/') and (next is none or '/')
        } else {
          return false;
        }
      }
    }

    return true;
  }

  public static boolean isMatched(CharSequence filter, CharSequence topic) {

    int filterIndex = 0;
    final int filterLength = filter.length();

    int topicIndex = 0;
    final int topicLength = topic.length();

    while (filterIndex < filterLength && topicIndex < topicLength) {
      final char filterChar = filter.charAt(filterIndex);
      final char topicChar = topic.charAt(topicIndex);
      if (filterChar == '#') {
        // skip to end of topic
        topicIndex = topicLength - 1;
      } else if (filterChar == '+') {
        // skip to next separator or end of topic
        int nextIndex = topicIndex + 1;
        while (nextIndex < topicLength && topic.charAt(nextIndex) != '/') {
          nextIndex = ++topicIndex + 1;
        }
      } else if (filterChar != topicChar) {
        // characters mismatch
        return false;
      }
      filterIndex++;
      topicIndex++;
    }

    switch (filterLength - filterIndex) {
      case 1:
        // filter has remaining "#"
        return filter.charAt(filterIndex) == '#';
      case 2:
        // filter has remaining "/#"
        return filter.charAt(filterIndex) == '/'
            && filter.charAt(filterIndex + 1) == '#';
      case 3:
        // filter has remaining "+/#"
        return filter.charAt(filterIndex) == '+'
            && filter.charAt(filterIndex + 1) == '/'
            && filter.charAt(filterIndex + 2) == '#';

      default:
        return (topicIndex == topicLength) && (filterIndex == filterLength);
    }
  }

  public static boolean isValidShareName(CharSequence shareName) {
    final int length = shareName.length();
    for (int index = 0; index < length; index++) {
      final char current = shareName.charAt(index);
      if (current == NUL || current == '/' || current == '#' || current == '+') {
        return false;
      }
    }
    return true;
  }

  public static String toSharedFilter(CharSequence shareName, CharSequence filter) {
    return new StringBuilder(8 + shareName.length() + filter.length())
        .append("$share/").append(shareName).append('/').append(filter).toString();
  }
}
