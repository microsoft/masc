package org.apache.accumulo.spark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.Text;

public class AvroRowEncoderIterator extends BaseMappingIterator {
  private Schema schema;
  private ByteArrayOutputStream binaryBuffer = new ByteArrayOutputStream();
  private DatumWriter<GenericRecord> writer;
  private BinaryEncoder encoder;
  private GenericRecord record;

  @Override
  protected void startRow(Text rowKey) throws IOException {
    binaryBuffer.reset();
    record = new GenericData.Record(schema);
  }

  @Override
  protected void processCell(Key key, Value value, String column, Object decodedValue)
      throws IOException {
    // TODO: instead of column use the index, though it needs to be passed into the
    // global dictionary...
    record.put(column, decodedValue);
  }

  @Override
  protected byte[] endRow() throws IOException {
    writer.write(record, encoder);
    encoder.flush();
    binaryBuffer.flush();

    return binaryBuffer.toByteArray();
  }

  private Schema.Type getAvroType(String type) {
    return Schema.Type.valueOf(type);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {

    super.init(source, options, env);

    // construct schema
    List<Schema.Field> fields = new ArrayList<>();
    for (Map.Entry<String,SchemaMappingField> entry : schemaMapping.getMapping().entrySet()) {
      SchemaMappingField field = entry.getValue();

      fields.add(new Schema.Field(entry.getKey(), Schema.create(getAvroType(field.getType())), null,
          (Object) null));
    }

    // setup serialization
    schema = Schema.createRecord(fields);
    writer = new SpecificDatumWriter<>(schema);
    encoder = EncoderFactory.get().binaryEncoder(binaryBuffer, null);
  }
}
