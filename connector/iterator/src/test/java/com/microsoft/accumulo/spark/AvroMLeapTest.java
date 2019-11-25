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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import com.microsoft.accumulo.spark.processors.AvroRowMLeap;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.google.common.io.Resources;

public class AvroMLeapTest {

  private AvroRowEncoderIterator createIterator(String mleapFilter) throws IOException {
    // load mleap model
    byte[] mleapBundle = Resources.toByteArray(AvroMLeapTest.class.getResource("pyspark.lr.zip"));
    String mleapBundleBase64 = Base64.getEncoder().encodeToString(mleapBundle);

    SortedMap<Key, Value> map = new TreeMap<>();
    map.put(new Key("key1", "cf1", "cq1"), new Value(new DoubleLexicoder().encode(0.0)));
    map.put(new Key("key2", "cf1", "cq1"), new Value(new DoubleLexicoder().encode(8.2)));

    SortedMapIterator parentIterator = new SortedMapIterator(map);
    AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

    Map<String, String> options = new HashMap<>();
    options.put(AvroRowEncoderIterator.SCHEMA,
        "[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"t\":\"double\",\"fvn\":\"v0\"},{\"cf\":\"cf1\",\"cq\":\"cq2\",\"t\":\"string\"}]");

    // pass the model to the iterator
    options.put(AvroRowMLeap.MLEAP_BUNDLE, mleapBundleBase64);

    // map cf1.cq1 to fit the models input data frame
    options.put("column.feature.double", "${cf1.cq1}");

    if (StringUtils.isNotBlank(mleapFilter))
      options.put(AvroRowEncoderIterator.MLEAP_FILTER, mleapFilter);

    iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
    iterator.seek(new Range(), AvroUtil.EMPTY_SET, false);

    return iterator;
  }

  @Test
  public void testMLeapModelExecution() throws IOException {
    AvroRowEncoderIterator iterator = createIterator(null);

    // row 1
    assertTrue(iterator.hasTop());
    GenericRecord record = AvroUtil.deserialize(iterator.getTopValue().get(), iterator.getSchema());

    assertEquals("key1", iterator.getTopKey().getRow().toString());
    assertEquals(-0.08748407856807701, (double) record.get("prediction"), 0.00001);

    // row2
    iterator.next();

    assertTrue(iterator.hasTop());
    record = AvroUtil.deserialize(iterator.getTopValue().get(), iterator.getSchema());

    assertEquals("key2", iterator.getTopKey().getRow().toString());
    assertEquals(0.8827512234363478, (double) record.get("prediction"), 0.00001);

    // end
    iterator.next();
    assertFalse(iterator.hasTop());
  }

  @Test
  public void testMLeapModelPredictionFiltering() throws IOException {
    AvroRowEncoderIterator iterator = createIterator("${prediction > 0.7}");

    assertTrue(iterator.hasTop());
    assertEquals("key2", iterator.getTopKey().getRow().toString());

    // end
    iterator.next();
    assertFalse(iterator.hasTop());
  }
}
