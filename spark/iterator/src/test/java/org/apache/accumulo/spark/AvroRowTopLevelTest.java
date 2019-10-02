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

package org.apache.accumulo.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.spark.record.AvroSchemaBuilder;
import org.apache.accumulo.spark.record.RowBuilderField;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class AvroRowTopLevelTest {
  @Test
  public void testSchemaGeneration() {
    RowBuilderField[] schemaMappingFields = new RowBuilderField[] { new RowBuilderField("cf1", "cq1", "long", "v0"),
        new RowBuilderField("cf2", null, "double", "v1") };

    Schema schema = AvroSchemaBuilder.buildSchema(Arrays.asList(schemaMappingFields));

    assertEquals(Type.RECORD, schema.getType());
    assertEquals(2, schema.getFields().size());

    Field f0 = schema.getFields().get(0);

    // cf1 nested record
    assertEquals(Type.RECORD, f0.schema().getType());
    assertEquals(1, f0.schema().getFields().size());

    // cf1.cq1 nested field
    Field f00 = f0.schema().getFields().get(0);
    assertTrue(f00.schema().isUnion());
    assertTrue(f00.schema().isNullable());

    // nullable long
    assertEquals(2, f00.schema().getTypes().size());
    assertEquals(Type.NULL, f00.schema().getTypes().get(0).getType());
    assertEquals(Type.LONG, f00.schema().getTypes().get(1).getType());

    // cf2 top-level field
    Field f1 = schema.getFields().get(1);

    // nullable double
    assertTrue(f1.schema().isUnion());
    assertTrue(f1.schema().isNullable());
    assertEquals(2, f1.schema().getTypes().size());
    assertEquals(Type.DOUBLE, f1.schema().getTypes().get(1).getType());
  }

  @Test
  public void testTopLevelFields() throws IOException {
    SortedMap<Key, Value> map = new TreeMap<>();
    map.put(new Key("key1", "cf1", "cq1"), new Value(new LongLexicoder().encode(3L)));
    map.put(new Key("key1", "cf2", ""), new Value("abc"));

    map.put(new Key("key2", "cf2"), new Value("def"));

    SortedMapIterator parentIterator = new SortedMapIterator(map);
    AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

    Map<String, String> options = new HashMap<>();
    options.put(AvroRowEncoderIterator.SCHEMA,
        "[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"t\":\"long\"},{\"cf\":\"cf2\",\"t\":\"STRING\"}]");

    iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
    iterator.seek(new Range(), AvroUtil.EMPTY_SET, false);

    RowBuilderField[] schemaMappingFields = new RowBuilderField[] { new RowBuilderField("cf1", "cq1", "long", "v0"),
        new RowBuilderField("cf2", null, "string", "v1") };

    Schema schema = AvroSchemaBuilder.buildSchema(Arrays.asList(schemaMappingFields));

    // ############################## ROW 1
    assertTrue(iterator.hasTop());
    assertEquals("key1", iterator.getTopKey().getRow().toString());

    // validate value
    byte[] data = iterator.getTopValue().get();

    GenericRecord record = AvroUtil.deserialize(data, schema);
    GenericRecord cf1Record = (GenericRecord) record.get("cf1");

    assertEquals(3L, cf1Record.get("cq1"));
    assertEquals("abc", record.get("cf2").toString());
    assertTrue(record.get("cf2") instanceof Utf8);

    // ############################## ROW 2
    iterator.next();

    assertTrue(iterator.hasTop());
    assertEquals("key2", iterator.getTopKey().getRow().toString());

    // validate value
    data = iterator.getTopValue().get();

    record = AvroUtil.deserialize(data, schema);
    cf1Record = (GenericRecord) record.get("cf1");

    assertNull(cf1Record.get("cq1"));
    assertEquals("def", record.get("cf2").toString());
    assertTrue(record.get("cf2") instanceof Utf8);

    // End of data
    iterator.next();
    assertFalse(iterator.hasTop());
  }
}
