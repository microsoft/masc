/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.spark.juel;

import java.nio.charset.StandardCharsets;

import org.apache.avro.util.Utf8;

/**
 * Wrap zero-copy Avro Utf8 class and extend with string filter support.
 */
public class AvroUtf8Wrapper extends Utf8 {

  // lazy initialized string
  private String string;

  public AvroUtf8Wrapper(byte[] data) {
    super(data);
  }

  public String getString() {
    if (this.string == null) {
      byte[] bytes = getBytes();
      this.string = new String(bytes, 0, bytes.length, StandardCharsets.UTF_8);
    }

    return this.string;
  }

  public boolean endsWith(String postfix) {
    return getString().endsWith(postfix);
  }

  public boolean startsWith(String prefix) {
    return getString().startsWith(prefix);
  }

  public boolean contains(String text) {
    return getString().contains(text);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof String) {
      return getString().equals(other);
    }

    return super.equals(other);
  }
}
