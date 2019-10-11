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

package org.apache.accumulo.iterator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Test;

public class DuplicationIteratorTest {
	private static final Collection<ByteSequence> EMPTY_SET = new HashSet<>();

	@Test
	public void testDuplication() throws IOException {
		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "cf1", "cq1"), new Value("Hello"));
		map.put(new Key("key1", "cf2", ""), new Value("abc"));

		map.put(new Key("key2", "cf2"), new Value("def"));

		SortedMapIterator parentIterator = new SortedMapIterator(map);
		DuplicationIterator iterator = new DuplicationIterator();

		Map<String, String> options = new HashMap<>();
		options.put(DuplicationIterator.OPTION_COUNT, "2");

		iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
		iterator.seek(new Range(), EMPTY_SET, false);

		String[] expectedStrings = {
				// key 1 (0)
				"key1_0 cf1 cq1 Hello",
				// key 1 (0)
				"key1_0 cf2  abc",
				// key 1 (1)
				"key1_1 cf1 cq1 Hello",
				// key 1 (1)
				"key1_1 cf2  abc",
				// key 1 (2)
				"key1_2 cf1 cq1 Hello",
				// key 1 (2)
				"key1_2 cf2  abc",
				// key 2 (0)
				"key2_0 cf2  def",
				// key 2 (1)
				"key2_1 cf2  def",
				// key 2 (2)
				"key2_2 cf2  def" };

		for (int i = 0; i < expectedStrings.length; i++) {
			assertTrue(iterator.hasTop());

			String actual = String.format("%s %s %s %s", iterator.getTopKey().getRow().toString(),
					iterator.getTopKey().getColumnFamily().toString(),
					iterator.getTopKey().getColumnQualifier().toString(), iterator.getTopValue().toString());

			assertEquals(expectedStrings[i], actual);

			iterator.next();
		}

		assertFalse(iterator.hasTop());
	}
}