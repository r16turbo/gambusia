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

package io.gambusia.mqtt;

public final class MqttTopics {
  private MqttTopics() {}

  public static final int MIN_CHARS = 1;
  public static final int MAX_CHARS = 65535;

  public static final char NUL = '\u0000';

  public static boolean isValidTopic(CharSequence topic) {
    final int length = topic.length();
    if (length < MIN_CHARS || length > MAX_CHARS) {
      return false; // no character, or more than 65535 character
    }
    for (int index = 0; index < length; index++) {
      final char current = topic.charAt(index);
      if (current == NUL || current == '#' || current == '+') {
        return false; // 'NUL' or wildcard characters
      }
    }
    return true;
  }

  public static boolean isValidFilter(CharSequence filter) {
    final int length = filter.length();
    if (length < MIN_CHARS || length > MAX_CHARS) {
      return false; // no character, or more than 65535 character
    }
    for (int index = 0; index < length; index++) {
      final char current = filter.charAt(index);
      if (current == NUL) {
        return false; // current is 'NUL'
      } else if (current == '#' && ((index > 0 && filter.charAt(index - 1) != '/')
          || (index + 1 < length))) {
        return false; // (previous is not none or '/') or (next is not none)
      } else if (current == '+' && ((index > 0 && filter.charAt(index - 1) != '/')
          || (index + 1 < length && filter.charAt(index + 1) != '/'))) {
        return false; // (previous is not none or '/') or (next is not none or '/')
      }
    }
    return true;
  }

  public static <T extends CharSequence> T requireValidTopic(T topic, String name)
      throws NullPointerException, IllegalArgumentException {
    if (topic == null) {
      throw new NullPointerException(name);
    } else if (!isValidTopic(topic)) {
      throw new IllegalArgumentException(name);
    }
    return topic;
  }

  public static <F extends CharSequence> F requireValidFilter(F filter, String name)
      throws NullPointerException, IllegalArgumentException {
    if (filter == null) {
      throw new NullPointerException(name);
    } else if (!isValidFilter(filter)) {
      throw new IllegalArgumentException(name);
    }
    return filter;
  }

  public static boolean matches(CharSequence filter, CharSequence topic) {

    int filterIndex = 0;
    final int filterLength = filter.length();

    int topicIndex = 0;
    final int topicLength = topic.length();

    while (filterIndex < filterLength && topicIndex < topicLength) {
      final char filterChar = filter.charAt(filterIndex++);
      final char topicChar = topic.charAt(topicIndex++);
      if (filterChar == '#') {
        // matches any number of subsequent topics
        return true;
      } else if (filterChar == '+') {
        // skip to next separator or end of topic
        if (topicChar != '/') {
          while (topicIndex < topicLength && topic.charAt(topicIndex) != '/') {
            topicIndex++;
          }
        } else if (filterIndex < filterLength) {
          // if filter is valid, topic level matches
          filterIndex++;
        } else {
          // topic level mismatch
          return false;
        }
      } else if (filterChar != topicChar) {
        // characters mismatch
        return false;
      }
    }

    switch (filterLength - filterIndex) {
      case 1:
        // filter has remaining "#"
        return filter.charAt(filterIndex) == '#';
      case 2:
        // filter has remaining "/#"
        // (if filter is valid, can skip '/' check)
        return filter.charAt(filterIndex + 1) == '#';
      case 3:
        // filter has remaining "+/#"
        // (if filter is valid, can skip '/' check)
        return filter.charAt(filterIndex) == '+'
            && filter.charAt(filterIndex + 2) == '#';

      default:
        return (topicIndex == topicLength) && (filterIndex == filterLength);
    }
  }

  public static boolean isValidShareName(CharSequence shareName) {
    final int length = shareName.length();
    if (length < MIN_CHARS || length > MAX_CHARS) {
      return false; // no character, or more than 65535 character
    }
    for (int index = 0; index < length; index++) {
      final char current = shareName.charAt(index);
      if (current == NUL || current == '/' || current == '#' || current == '+') {
        return false; // 'NUL' or topic level separator, wildcard characters
      }
    }
    return true;
  }

  public static <S extends CharSequence> S requireValidShareName(S shareName, String name)
      throws NullPointerException, IllegalArgumentException {
    if (shareName == null) {
      throw new NullPointerException(name);
    } else if (!isValidShareName(shareName)) {
      throw new IllegalArgumentException(name);
    }
    return shareName;
  }

  public static String toSharedFilter(CharSequence shareName, CharSequence filter) {
    return new StringBuilder(8 + shareName.length() + filter.length())
        .append("$share/").append(shareName).append('/').append(filter).toString();
  }
}
