package org.apache.accumulo.spark;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;

public class AvroFilterTest {

	private static void validateFilter(String filter, String... expectedKeys) throws IOException {
		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "cf1", "cq1"), new Value("3"));
		map.put(new Key("key1", "cf2", ""), new Value("abc"));

		map.put(new Key("key2", "cf2"), new Value("def"));

		SortedMapIterator parentIterator = new SortedMapIterator(map);
		AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

		Map<String, String> options = new HashMap<>();
		options.put(AvroRowEncoderIterator.SCHEMA,
				"[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"t\":\"long\",\"fvn\":\"v0\"},{\"cf\":\"cf2\",\"t\":\"STRING\",\"fvn\":\"v1\"}]");

		options.put(AvroRowEncoderIterator.FILTER, filter);

		iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
		iterator.seek(new Range(), AvroTestUtil.EMPTY_SET, false);

		// collect rows
		List<String> foundRows = new ArrayList<>();
		for (; iterator.hasTop(); iterator.next())
			foundRows.add(iterator.getTopKey().getRow().toString());

		assertArrayEquals(expectedKeys, foundRows.toArray(new String[0]));
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
}