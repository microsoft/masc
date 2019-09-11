package org.apache.accumulo.spark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.Text;

public class AvroRowEncoderIterator extends BaseMappingIterator {

  class EnhancedGenericRecordBuilder {
    // used for fast clear
    private Field[] nestedFields;
    // wrapped builder
    private GenericRecordBuilder recordBuilder;
    // used for fast value setting
    private Map<Text, Field> fieldLookup;
    // co-relate back to parent record
    private Field parentField;

    public EnhancedGenericRecordBuilder(Schema schema, Field parentField) {
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

    public Record build() {
      return recordBuilder.build();
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
  private Map<Text, EnhancedGenericRecordBuilder> columnFamilyRecordBuilder = new HashMap<>();

  // allocate once to re-use memory
  private Text columnFamilyText = new Text();
  private Text columnQualifierText = new Text();

  @Override
  protected void startRow(Text rowKey) throws IOException {
    binaryBuffer.reset();

    // clear all fields
    for (EnhancedGenericRecordBuilder builder : columnFamilyRecordBuilder.values())
      builder.clear();
  }

  @Override
  protected void processCell(Key key, Value value, Object decodedValue) throws IOException {
    // passing columnFamilyText to re-use memory
    EnhancedGenericRecordBuilder builder = columnFamilyRecordBuilder.get(key.getColumnFamily(columnFamilyText));

    // passing columnQualifierText to re-use memroy
    builder.set(key.getColumnQualifier(columnQualifierText), decodedValue);
  }

  @Override
  protected byte[] endRow() throws IOException {
    // populate root record
    for (EnhancedGenericRecordBuilder nestedRecordBuilder : columnFamilyRecordBuilder.values())
      rootRecordBuilder.set(nestedRecordBuilder.parentField, nestedRecordBuilder.build());

    writer.write(rootRecordBuilder.build(), encoder);
    encoder.flush();
    binaryBuffer.flush();

    return binaryBuffer.toByteArray();
  }

  private SchemaBuilder.FieldAssembler<Schema> addColumnQualifierFields(SchemaBuilder.FieldAssembler<Schema> builder,
      List<SchemaMappingField> fields) {
    for (SchemaMappingField schemaMappingField : fields) {
      switch (schemaMappingField.getType().toUpperCase()) {
      case "STRING":
        builder = builder.optionalString(schemaMappingField.getColumnQualifier());
        break;

      case "LONG":
        builder = builder.optionalLong(schemaMappingField.getColumnQualifier());
        break;

      case "INTEGER":
        builder = builder.optionalInt(schemaMappingField.getColumnQualifier());
        break;

      case "DOUBLE":
        builder = builder.optionalDouble(schemaMappingField.getColumnQualifier());
        break;

      case "FLOAT":
        builder = builder.optionalFloat(schemaMappingField.getColumnQualifier());
        break;

      case "BOOLEAN":
        builder = builder.optionalBoolean(schemaMappingField.getColumnQualifier());
        break;

      case "BYTES":
        builder = builder.optionalBytes(schemaMappingField.getColumnQualifier());
        break;

      default:
        throw new IllegalArgumentException("Unsupported type '" + schemaMappingField.getType() + "'");
      }
    }

    return builder;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
      throws IOException {

    super.init(source, options, env);

    // construct schema
    // List<Schema.Field> fields = new ArrayList<>();
    SchemaBuilder.FieldAssembler<Schema> rootAssembler = SchemaBuilder.record("root").fields();

    // group fields by column family
    Map<String, List<SchemaMappingField>> groupedByColumnFamily = Stream.of(schemaMappingFields)
        .collect(Collectors.groupingBy(SchemaMappingField::getColumnFamily));

    // loop over column families
    for (Map.Entry<String, List<SchemaMappingField>> entry : groupedByColumnFamily.entrySet()) {

      // loop over column qualifiers
      SchemaBuilder.FieldAssembler<Schema> columnFieldsAssembler = SchemaBuilder.record(entry.getKey()).fields();
      columnFieldsAssembler = addColumnQualifierFields(columnFieldsAssembler, entry.getValue());

      // add nested type to to root assembler
      rootAssembler = rootAssembler.name(entry.getKey()).type(columnFieldsAssembler.endRecord()).noDefault();
    }

    // setup serialization
    schema = rootAssembler.endRecord();
    writer = new SpecificDatumWriter<>(schema);
    encoder = EncoderFactory.get().binaryEncoder(binaryBuffer, null);

    // separate record builder for the root record holding the nested schemas
    rootRecordBuilder = new GenericRecordBuilder(schema);

    // setup GenericRecordBuilder for each column family
    for (Field field : schema.getFields()) {
      Schema nestedSchema = schema.getField(field.name()).schema();

      // store as map for fast lookup
      columnFamilyRecordBuilder.put(new Text(field.name()), new EnhancedGenericRecordBuilder(nestedSchema, field));
    }
  }
}
