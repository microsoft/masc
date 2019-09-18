package org.apache.accumulo.spark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.spark.el.AvroContext;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.Text;

public class AvroRowEncoderIterator extends BaseMappingIterator {
  public static final String FILTER = "filter";

  interface SubGenericRecordBuilder {
    void set(Text columnQualifier, Object value);

    void endRow();

    void clear();
  }

  class NestedGenericRecordBuilder implements SubGenericRecordBuilder {
    // used for fast clear
    private Field[] nestedFields;
    // wrapped builder
    private GenericRecordBuilder recordBuilder;
    // used for fast value setting
    private Map<Text, Field> fieldLookup;
    // co-relate back to parent record
    private Field parentField;

    public NestedGenericRecordBuilder(Schema schema, Field parentField) {
      this.parentField = parentField;

      nestedFields = schema.getFields().toArray(new Field[0]);

      recordBuilder = new GenericRecordBuilder(schema);

      // make sure we made from Text (not string) to field
      // a) this avoids memory allocation for the string object
      // b) this allows use to get the field index in the avro record directly (no
      // field index lookup required)
      fieldLookup = Stream.of(nestedFields).collect(Collectors.toMap(f -> new Text(f.name()), Function.identity()));
    }

    public Field getParentField() {
      return parentField;
    }

    public void set(Text columnQualifer, Object value) {
      // from hadoop text to field pos
      recordBuilder.set(fieldLookup.get(columnQualifer), value);
    }

    public void clear() {
      for (Field field : nestedFields)
        recordBuilder.clear(field);
    }

    public void endRow() {
      rootRecordBuilder.set(parentField, recordBuilder.build());
    }
  }

  class TopLevelGenericRecordBuilder implements SubGenericRecordBuilder {

    private Field field;

    public TopLevelGenericRecordBuilder(Field field) {
      this.field = field;
    }

    public void set(Text columnQualifier, Object value) {
      rootRecordBuilder.set(this.field, value);
    }

    public void endRow() {
    }

    public void clear() {
    }
  }

  // root schema holding fields for each column family
  private Schema schema;

  // avro writer infra
  private ByteArrayOutputStream binaryBuffer = new ByteArrayOutputStream();
  private DatumWriter<GenericRecord> writer;
  private BinaryEncoder encoder;

  // record builder for the root
  private GenericRecordBuilder rootRecordBuilder;
  // fast lookup by using Text as key type
  private Map<Text, SubGenericRecordBuilder> columnFamilyRecordBuilder = new HashMap<>();

  // allocate once to re-use memory
  private Text columnFamilyText = new Text();
  private Text columnQualifierText = new Text();
  private AvroContext filterContext;
  private ValueExpression filterExpression;

  @Override
  protected void startRow(Text rowKey) throws IOException {
    binaryBuffer.reset();

    // clear all fields
    for (SubGenericRecordBuilder builder : columnFamilyRecordBuilder.values())
      builder.clear();
  }

  @Override
  protected void processCell(Key key, Value value, Object decodedValue) throws IOException {
    // passing columnFamilyText to re-use memory
    SubGenericRecordBuilder builder = columnFamilyRecordBuilder.get(key.getColumnFamily(columnFamilyText));

    // passing columnQualifierText to re-use memory
    builder.set(key.getColumnQualifier(columnQualifierText), decodedValue);
  }

  @Override
  protected byte[] endRow() throws IOException {
    // populate root record
    for (SubGenericRecordBuilder nestedRecordBuilder : columnFamilyRecordBuilder.values())
      nestedRecordBuilder.endRow();

    Record record = rootRecordBuilder.build();

    // evaluate the filter against the record
    if (filterExpression != null) {
      filterContext.setAvroRecord(record);

      if (!(boolean) filterExpression.getValue(filterContext))
        return null;
    }

    // serialize
    writer.write(record, encoder);
    encoder.flush();
    binaryBuffer.flush();

    return binaryBuffer.toByteArray();
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
      throws IOException {

    super.init(source, options, env);

    // avro serialization setup
    schema = AvroUtil.buildSchema(schemaMappingFields);

    writer = new SpecificDatumWriter<>(schema);
    encoder = EncoderFactory.get().binaryEncoder(binaryBuffer, null);

    // separate record builder for the root record holding the nested schemas
    rootRecordBuilder = new GenericRecordBuilder(schema);

    // setup GenericRecordBuilder for each column family
    for (Field field : schema.getFields()) {
      Schema nestedSchema = field.schema();

      // nested vs top level element
      SubGenericRecordBuilder builder = nestedSchema.getType() == Type.RECORD
          ? new NestedGenericRecordBuilder(nestedSchema, field)
          : new TopLevelGenericRecordBuilder(field);

      // store in map for fast lookup
      columnFamilyRecordBuilder.put(new Text(field.name()), builder);
    }

    // filter setup
    String filter = options.get(FILTER);
    if (filter != null) {
      ExpressionFactory factory = ExpressionFactory.newInstance();

      filterContext = new AvroContext(schema, schemaMappingFields);

      filterExpression = factory.createValueExpression(filterContext, filter, boolean.class);
    }
  }
}
