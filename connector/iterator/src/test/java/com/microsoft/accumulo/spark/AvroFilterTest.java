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

package com.microsoft.accumulo.spark;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Test;

public class AvroFilterTest {

  private static void validateFilter(String filter, String... expectedKeys) throws IOException {
    SortedMap<Key, Value> map = new TreeMap<>();
    map.put(new Key("key1", "cf1", "cq1"), new Value(new LongLexicoder().encode(3L)));
    map.put(new Key("key1", "cf1", "cq2"), new Value("Hello"));
    map.put(new Key("key1", "cf2", ""), new Value("abc"));

    map.put(new Key("key2", "cf2"), new Value("def"));

    SortedMapIterator parentIterator = new SortedMapIterator(map);
    AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

    Map<String, String> options = new HashMap<>();
    options.put(AvroRowEncoderIterator.SCHEMA,
        "[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"t\":\"long\",\"fvn\":\"v0\"},{\"cf\":\"cf1\",\"cq\":\"cq2\",\"t\":\"string\"},{\"cf\":\"cf2\",\"t\":\"STRING\",\"fvn\":\"v1\"}]");

    options.put(AvroRowEncoderIterator.FILTER, filter);

    // include computed column
    options.put("column.vc1.long", "${cf1.cq1 + 5}");

    iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
    iterator.seek(new Range(), AvroUtil.EMPTY_SET, false);

    // collect rows
    List<String> foundRows = new ArrayList<>();
    for (; iterator.hasTop(); iterator.next())
      foundRows.add(iterator.getTopKey().getRow().toString());

    assertArrayEquals(expectedKeys, foundRows.toArray(new String[0]));
  }

  @Test
  public void testComputedColumn() throws IOException {
    validateFilter("${vc1 == 8}", "key1");
  }

  @Test
  public void testEquals() throws IOException {
    validateFilter("${v0 == 3}", "key1");
    validateFilter("${v0 != 2}", "key1", "key2");
    validateFilter("${v1 == 'def'}", "key2");
  }

  @Test
  public void testIsNull() throws IOException {
    validateFilter("${v0 == null}", "key2");
  }

  @Test
  public void testEndsWith() throws IOException {
    // test on variable
    validateFilter("${v1.endsWith('ef')}", "key2");

    // test on object/property combination
    validateFilter("${cf1.cq2.endsWith('ello')}", "key1");
  }

  @Test
  public void testStartsWith() throws IOException {
    // test on variable
    validateFilter("${v1.startsWith('de')}", "key2");

    // test on object/property combination
    validateFilter("${cf1.cq2.startsWith('Hel')}", "key1");
  }

  @Test
  public void testContains() throws IOException {
    // test on variable
    validateFilter("${v1.contains('d')}", "key2");

    // test on object/property combination
    validateFilter("${cf1.cq2.contains('ell')}", "key1");
  }

  @Test
  public void testIn() throws IOException {
    // test on variable
    validateFilter("${v1.in('aaa','def')}", "key2");

    // test on object/property combination
    validateFilter("${cf1.cq2.in('A','Hello','xxx')}", "key1");

    // test on object/property combination
    validateFilter("${cf1.cq2.in('Hello')}", "key1");
  }
}
