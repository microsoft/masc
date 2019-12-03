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

package com.microsoft.accumulo.spark.record;

import java.util.Date;

import org.apache.accumulo.core.client.lexicoder.DateLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.Encoder;
import org.apache.accumulo.core.client.lexicoder.FloatLexicoder;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;

/**
 * Link JSON type string with Java class and Lexicoder
 */
public enum RowBuilderType {
  String(String.class, new StringLexicoder()), Integer(int.class, new IntegerLexicoder()),
  Long(long.class, new LongLexicoder()), Float(float.class, new FloatLexicoder()),
  Double(double.class, new DoubleLexicoder()), Date(Date.class, new DateLexicoder()), Boolean(boolean.class, null),
  Bytes(byte[].class, new ByteEncoder()), Unknown(null, null);

  private static class ByteEncoder implements Encoder<byte[]> {

    @Override
    public byte[] encode(byte[] object) {
      return object;
    }

    @Override
    public byte[] decode(byte[] bytes) throws IllegalArgumentException {
      return bytes;
    }
  }

  private Class<?> javaClass;
  private Encoder<?> encoder;

  RowBuilderType(Class<?> javaClass, Encoder<?> encoder) {
    this.javaClass = javaClass;
    this.encoder = encoder;
  }

  public static RowBuilderType valueOfIgnoreCase(String name) {
    for (RowBuilderType type : RowBuilderType.values()) {
      if (name.equalsIgnoreCase(type.name()))
        return type;
    }

    return null;
  }

  /**
   * @return the javaClass
   */
  public Class<?> getJavaClass() {
    return javaClass;
  }

  /**
   * @return the encoder
   */
  public Encoder<?> getEncoder() {
    return encoder;
  }
}
