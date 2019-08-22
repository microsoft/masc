// package org.apache.accumulo.spark;

// import static org.junit.Assert.assertEquals;
// import static org.junit.Assert.assertNotNull;

// import java.io.ByteArrayOutputStream;
// import java.util.Arrays;
// import java.util.List;

// import org.apache.avro.Schema;
// import org.apache.avro.generic.GenericData;
// import org.apache.avro.generic.GenericRecord;
// import org.apache.avro.io.BinaryDecoder;
// import org.apache.avro.io.BinaryEncoder;
// import org.apache.avro.io.DatumReader;
// import org.apache.avro.io.DatumWriter;
// import org.apache.avro.io.DecoderFactory;
// import org.apache.avro.io.EncoderFactory;
// import org.apache.avro.specific.SpecificDatumReader;
// import org.apache.avro.specific.SpecificDatumWriter;
// import org.apache.avro.util.Utf8;
// import org.junit.Test;

// import com.fasterxml.jackson.databind.ObjectMapper;

// public class DataFrameIteratorTest {

// // test to check appraisal
// @Test
// public void testAvro() throws Exception {
// List<Schema.Field> fields = Arrays.asList(
// new Schema.Field("f1", Schema.create(Schema.Type.INT), null, null),
// new Schema.Field("f2", Schema.create(Schema.Type.valueOf("STRING")), null,
// null));

// Schema schema = Schema.createRecord(fields);

// // serialize
// GenericRecord user1 = new GenericData.Record(schema);
// user1.put("f1", 5);
// user1.put("f2", "foo");

// ByteArrayOutputStream baos = new ByteArrayOutputStream();
// BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);

// DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);

// writer.write(user1, encoder);
// encoder.flush();
// baos.flush();

// byte[] data = baos.toByteArray();

// // Spark
// // deserialize
// DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
// BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

// GenericRecord user2 = new GenericData.Record(schema);

// reader.read(user2, decoder);

// assertEquals(5, user2.get("f1"));
// assertEquals("foo", ((Utf8) user2.get("f2")).toString());
// }

// @Test
// public void testJson() throws Exception {
// ObjectMapper objectMapper = new ObjectMapper();

// String json =
// "{\"rowKeyTargetColumn\":\"rowKey\",\"mapping\":{\"f1\":{\"columnFamily\":\"cf1\",\"columnQualifier\":\"cq1\",\"type\":\"STRING\"}}}";

// SchemaMapping mapping = objectMapper.readValue(json, SchemaMapping.class);

// assertEquals("rowKey", mapping.getRowKeyTargetColumn());
// assertEquals(1, mapping.getMapping().size());

// SchemaMappingField field = mapping.getMapping().get("f1");
// assertNotNull(field);
// assertEquals("cf1", field.getColumnFamily());
// assertEquals("cq1", field.getColumnQualifier());
// }
// }
