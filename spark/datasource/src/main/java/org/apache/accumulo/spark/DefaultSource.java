package org.apache.accumulo.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Logger;
import org.apache.spark.sql.avro.AvroDeserializer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jackson.map.ObjectMapper;

public class DefaultSource implements DataSourceV2, ReadSupport {
  protected static final Class<?> CLASS = DefaultSource.class;
  protected static final Logger log = Logger.getLogger(CLASS);

  public static class AccumuloInputPartitionReader
      implements InputPartitionReader<InternalRow>, Serializable {
    private static final long serialVersionUID = 1L;

    // https://github.com/apache/accumulo/blob/master/core/src/main/java/org/apache/accumulo/core/client/mapred/AbstractInputFormat.java#L723
    private Iterator<Map.Entry<Key,Value>> scannerIterator;
    private ScannerBase scannerBase;
    private InternalRow row;
    private AvroDeserializer deserializer;
    private DatumReader<GenericRecord> reader;
    private Schema avroSchema;

    private static String catalystSchemaToJson(StructType schema) {
      StructField[] fields = schema.fields();

      Map<String,SchemaMappingField> mappingFields = new HashMap<>();
      for (int i = 0; i < fields.length; i++) {
        StructField field = fields[i];
        SchemaMappingField mappingField = new SchemaMappingField();

        mappingField.setColumnFamily(field.metadata().getString("cf"));
        mappingField.setColumnQualifier(field.metadata().getString("cq"));
        // TODO: toUpperCase() is weird...
        mappingField.setType(catalystToAvroType(fields[i].dataType()).getName().toUpperCase());

        mappingFields.put(field.name(), mappingField);
      }

      SchemaMapping schemaMapping = new SchemaMapping();
      schemaMapping.setMapping(mappingFields);

      try {
        return new ObjectMapper().writeValueAsString(schemaMapping);
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
    }

    private static Schema catalystSchemaToAvroSchema(StructType schema) {
      // compile-time method binding. yes it's deprecated. yes it's the only version
      // available in the spark version deployed
      StructField[] fields = schema.fields();
      List<Schema.Field> avroFields = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        avroFields.add(new Schema.Field(fields[i].name(),
            Schema.create(catalystToAvroType(fields[i].dataType())), (String) null,
            (org.codehaus.jackson.JsonNode) null));
      }

      // Arrays.asList(new Schema.Field("f1", Schema.create(Schema.Type.STRING),
      // (String) null,
      // (org.codehaus.jackson.JsonNode) null));

      return Schema.createRecord(avroFields);
    }

    public AccumuloInputPartitionReader(String tableName, Properties props, StructType schema) {
      Authorizations authorizations = new Authorizations();

      ClientContext client = new ClientContext(props);

      TableId tableId;
      try {
        tableId = Tables.getTableId(client, tableName);
      } catch (TableNotFoundException e) {
        // TODO
        // log.info(e);

        tableId = null;
      }

      Scanner scanner;
      scanner = new ScannerImpl(client, tableId, authorizations);

      IteratorSetting avroIterator = new IteratorSetting(20, "AVRO",
          "org.apache.accumulo.spark.AvroRowEncoderIterator");
      // String json =
      // "{\"rowKeyTargetColumn\":\"rowKey\",\"mapping\":{\"f1\":{\"columnFamily\":\"cf1\",\"columnQualifier\":\"cq1\",\"type\":\"STRING\"}}}";
      String json = catalystSchemaToJson(schema);
      // if (true)
      // throw new IllegalArgumentException(json);

      avroIterator.addOption("schema", json);

      scanner.addScanIterator(avroIterator);

      // TODO: ?
      // scanner.setRange(baseSplit.getRange());
      scannerBase = scanner;
      scannerIterator = scanner.iterator();

      // for debugging
      // StructType catalystSchema = new StructType(
      // new StructField[] { new StructField("f1", DataTypes.StringType, true,
      // Metadata.empty()) });

      // StringBuilder sb = new StringBuilder();
      // for (Constructor ctor : Schema.Field.class.getConstructors()) {
      // for (Class paramType : ctor.getParameterTypes())
      // sb.append(paramType + ", ");
      // sb.append("\n");
      // }

      // throw new IllegalArgumentException(sb.toString());

      // Avro
      avroSchema = catalystSchemaToAvroSchema(schema);
      deserializer = new AvroDeserializer(avroSchema, schema);

      reader = new SpecificDatumReader<>(avroSchema);
    }

    private static Schema.Type catalystToAvroType(DataType type) {
      if (type.equals(DataTypes.StringType))
        return Schema.Type.STRING;

      if (type.equals(DataTypes.IntegerType))
        return Schema.Type.INT;

      if (type.equals(DataTypes.FloatType))
        return Schema.Type.FLOAT;

      if (type.equals(DataTypes.DoubleType))
        return Schema.Type.DOUBLE;

      if (type.equals(DataTypes.BooleanType))
        return Schema.Type.BOOLEAN;

      if (type.equals(DataTypes.LongType))
        return Schema.Type.LONG;

      // Schema.Type.BYTES
      // ARRAY, MAP?

      throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    @Override
    public void close() throws IOException {
      if (scannerBase != null) {
        scannerBase.close();
        scannerBase = null;
      }
    }

    @Override
    public boolean next() throws IOException {

      if (!scannerIterator.hasNext())
        return false;

      Map.Entry<Key,Value> entry = scannerIterator.next();

      // TODO: handle key
      // key.set(currentKey = entry.getKey());

      byte[] data = entry.getValue().get();

      // byte[] -> avro
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
      GenericRecord avroRecord = new GenericData.Record(avroSchema);

      reader.read(avroRecord, decoder);

      // avro to catalyst
      row = (InternalRow) deserializer.deserialize(avroRecord);

      return true;
    }

    @Override
    public InternalRow get() {
      return row;
    }
  }

  public static class AccumuloDataSourceReader implements DataSourceReader, Serializable {
    private static final long serialVersionUID = 1L;
    private Properties props;
    private String tableName;
    private StructType schema;

    public AccumuloDataSourceReader(StructType schema, DataSourceOptions options) {
      this.schema = schema;
      tableName = options.tableName().get();

      props = new Properties();
      props.putAll(options.asMap());
    }

    @Override
    public StructType readSchema() {
      // TODO: parse from options?
      // return new StructType(new StructField[] { new StructField("f1",
      // DataTypes.StringType, true, Metadata.empty()) });
      return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
      // TODO:
      // https://github.com/apache/accumulo/blob/master/core/src/main/java/org/apache/accumulo/core/client/mapred/AbstractInputFormat.java#L723

      // return Arrays.asList(new AccumuloInputPartition());
      return Arrays.asList(new InputPartition<InternalRow>() {
        private static final long serialVersionUID = 1;

        @Override
        public InputPartitionReader<InternalRow> createPartitionReader() {

          // I assume this runs on the exectuors
          return new AccumuloInputPartitionReader(tableName, props, schema);
        }
      });
    }

  }

  @Override
  public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
    return new AccumuloDataSourceReader(schema, options);
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    // return new AccumuloDataSourceReader(options);
    throw new UnsupportedOperationException("must supply schema");
  }
}
