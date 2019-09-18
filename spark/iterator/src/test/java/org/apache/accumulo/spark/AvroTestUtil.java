package org.apache.accumulo.spark;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

public class AvroTestUtil {

	public static final Collection<ByteSequence> EMPTY_SET = new HashSet<>();

	public static GenericRecord deserialize(byte[] data, Schema schema) throws IOException {
		SpecificDatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

		return reader.read(null, decoder);
	}
}