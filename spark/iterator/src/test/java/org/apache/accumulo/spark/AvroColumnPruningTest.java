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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class AvroColumnPruningTest {
	@Test
	public void testColumnPruning() throws IOException {
		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "cf1", "cq1"), new Value(new LongLexicoder().encode(3L)));
		map.put(new Key("key1", "cf2", ""), new Value("abc"));

		map.put(new Key("key2", "cf2"), new Value("def"));

		SortedMapIterator parentIterator = new SortedMapIterator(map);
		AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

		Map<String, String> options = new HashMap<>();
		options.put(AvroRowEncoderIterator.SCHEMA,
				"[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"t\":\"long\"},{\"cf\":\"cf2\",\"t\":\"STRING\"}]");
		options.put(AvroRowEncoderIterator.PRUNED_COLUMNS, "cf2");

		iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
		iterator.seek(new Range(), AvroUtil.EMPTY_SET, false);

		RowBuilderField[] schemaMappingFields = new RowBuilderField[] {
				new RowBuilderField("cf2", null, "string", "v1") };

		Schema schema = AvroSchemaBuilder.buildSchema(Arrays.asList(schemaMappingFields));

		// ############################## ROW 1
		assertTrue(iterator.hasTop());
		assertEquals("key1", iterator.getTopKey().getRow().toString());

		// validate value
		byte[] data = iterator.getTopValue().get();

		GenericRecord record = AvroUtil.deserialize(data, schema);

		assertEquals("abc", record.get("cf2").toString());
		assertTrue(record.get("cf2") instanceof Utf8);

		// ############################## ROW 2
		iterator.next();

		assertTrue(iterator.hasTop());
		assertEquals("key2", iterator.getTopKey().getRow().toString());

		// validate value
		data = iterator.getTopValue().get();

		record = AvroUtil.deserialize(data, schema);

		assertEquals("def", record.get("cf2").toString());
		assertTrue(record.get("cf2") instanceof Utf8);

		// End of data
		iterator.next();
		assertFalse(iterator.hasTop());
	}
}
