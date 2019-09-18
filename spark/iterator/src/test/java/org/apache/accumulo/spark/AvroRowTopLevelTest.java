package org.apache.accumulo.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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

public class AvroRowTopLevelTest {
	@Test
	public void testSchemaGeneration() {
		SchemaMappingField[] schemaMappingFields = new SchemaMappingField[] {
				new SchemaMappingField("cf1", "cq1", "long", "v0"),
				new SchemaMappingField("cf2", null, "double", "v1") };

		Schema schema = AvroUtil.buildSchema(schemaMappingFields);

		assertEquals(
				"{\"type\":\"record\",\"name\":\"root\",\"fields\":[{\"name\":\"cf2\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"cf1\",\"type\":{\"type\":\"record\",\"name\":\"cf1\",\"fields\":[{\"name\":\"cq1\",\"type\":[\"null\",\"long\"],\"default\":null}]}}]}",
				schema.toString());
	}

	@Test
	public void testTopLevelFields() throws IOException {
		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "cf1", "cq1"), new Value("3"));
		map.put(new Key("key1", "cf2", ""), new Value("abc"));

		map.put(new Key("key2", "cf2"), new Value("def"));

		SortedMapIterator parentIterator = new SortedMapIterator(map);
		AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

		Map<String, String> options = new HashMap<>();
		options.put(AvroRowEncoderIterator.SCHEMA,
				"[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"t\":\"long\"},{\"cf\":\"cf2\",\"t\":\"STRING\"}]");

		iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
		iterator.seek(new Range(), AvroTestUtil.EMPTY_SET, false);

		SchemaMappingField[] schemaMappingFields = new SchemaMappingField[] {
				new SchemaMappingField("cf1", "cq1", "long", "v0"),
				new SchemaMappingField("cf2", null, "string", "v1") };

		Schema schema = AvroUtil.buildSchema(schemaMappingFields);

		// ############################## ROW 1
		assertTrue(iterator.hasTop());
		assertEquals("key1", iterator.getTopKey().getRow().toString());

		// validate value
		byte[] data = iterator.getTopValue().get();

		GenericRecord record = AvroTestUtil.deserialize(data, schema);
		GenericRecord cf1Record = (GenericRecord) record.get("cf1");

		assertEquals(3L, cf1Record.get("cq1"));
		assertEquals("abc", record.get("cf2").toString());

		// ############################## ROW 2
		iterator.next();

		assertTrue(iterator.hasTop());
		assertEquals("key2", iterator.getTopKey().getRow().toString());

		// validate value
		data = iterator.getTopValue().get();

		record = AvroTestUtil.deserialize(data, schema);
		cf1Record = (GenericRecord) record.get("cf1");

		assertNull(cf1Record.get("cq1"));
		assertEquals("def", record.get("cf2").toString());
	}
}