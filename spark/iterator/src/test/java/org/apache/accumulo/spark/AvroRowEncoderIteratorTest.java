
package org.apache.accumulo.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

public class AvroRowEncoderIteratorTest {

	private static final Collection<ByteSequence> EMPTY_SET = new HashSet<>();

	private GenericRecord deserialize(byte[] data, Schema schema) throws IOException {
		SpecificDatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

		return reader.read(null, decoder);
	}

	class MyRow {
		public String key;

		public String cf1cq1;

		MyRow(String key, String cf1cq1) {
			this.key = key;
			this.cf1cq1 = cf1cq1;
		}
	}

	private void validateSingleRowSimpleSchema(SortedMap<Key, Value> map, MyRow... expectedRows) throws IOException {
		SortedMapIterator parentIterator = new SortedMapIterator(map);

		// setup avro encoder iterator
		AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

		Map<String, String> options = new HashMap<>();
		options.put(AvroRowEncoderIterator.SCHEMA, "[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"t\":\"STRING\"}]");

		iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
		iterator.seek(new Range(), EMPTY_SET, false);

		// the expected avro schema
		Schema schema = SchemaBuilder.record("root").fields().name("cf1")
				.type(SchemaBuilder.record("cf1").fields().optionalString("cq1").endRecord()).noDefault().endRecord();

		for (MyRow row : expectedRows) {
			assertTrue(iterator.hasTop());

			// validate key
			assertEquals(row.key, iterator.getTopKey().getRow().toString());

			// validate value
			byte[] data = iterator.getTopValue().get();

			GenericRecord record = deserialize(data, schema);
			GenericRecord cf1Record = (GenericRecord) record.get("cf1");

			assertEquals(row.cf1cq1, cf1Record.get("cq1").toString());

			// move to next
			iterator.next();
		}

		assertFalse(iterator.hasTop());
	}

	@Test
	public void testSingleFieldString() throws IOException {
		// setup input iterator
		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "cf1", "cq1"), new Value("abc"));

		validateSingleRowSimpleSchema(map, new MyRow("key1", "abc"));
	}

	@Test
	public void testSkippedField1() throws IOException {
		// setup input iterator
		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "cf1", "cq1"), new Value("abc"));
		map.put(new Key("key1", "cf1", "cq2"), new Value("def"));

		validateSingleRowSimpleSchema(map, new MyRow("key1", "abc"));
	}

	@Test
	public void testSkippedField2() throws IOException {
		// setup input iterator
		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "cf0", "cq1"), new Value("xxx"));
		map.put(new Key("key1", "cf1", "cq1"), new Value("abc"));
		map.put(new Key("key1", "cf1", "cq2"), new Value("def"));

		validateSingleRowSimpleSchema(map, new MyRow("key1", "abc"));
	}

	@Test
	public void testMultipleRows() throws IOException {
		// setup input iterator
		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "cf1", "cq1"), new Value("xxx"));
		map.put(new Key("key2", "cf0", "cq1"), new Value("abc"));
		map.put(new Key("key3", "cf1", "cq1"), new Value("yyy"));

		validateSingleRowSimpleSchema(map, new MyRow("key1", "xxx"), new MyRow("key3", "yyy"));
	}
}