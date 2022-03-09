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

package io.gambusia.mqtt.handler.internal;

public abstract class Hash {

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Hash; // to only class, because check with a hashCode
  }

  public static final class MutableHash extends Hash {

    private int hashCode = 0;

    public Hash set(int hashCode) {
      this.hashCode = hashCode;
      return this;
    }

    @Override
    public int hashCode() {
      return hashCode; // expect to cache the ConcurrentHashMap
    }
  }

  public static final class ImmutableHash extends Hash {

    private final int hashCode;

    public ImmutableHash(int hashCode) {
      this.hashCode = hashCode;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
