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

package com.microsoft.accumulo.spark.record;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.lexicoder.Encoder;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import com.microsoft.accumulo.spark.juel.AvroUtf8Wrapper;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;

/**
 * This class collects all cells of interest into an AVRO Generic Record.
 * 
 * Cells with non-empty column family and column qualifier are stored in nested
 * AVRO records. Cells with empty column qualifier are stored in the top-level
 * record.
 * 
 * Example:
 * 
 * <pre>
 * cf1, cq1,  abc
 * cf1, cq2,  3.2
 * cf2, null, 6
 * cf3, cq3,  def
 * </pre>
 * 
 * Avro Record:
 * 
 * <pre>
 * { 
 * 	 cf1: { cq1: "abc", cq2: 3.2 }, 
 * 	 cf2: 6, 
 *   cf3: { cq3: "def " }
 * }
 * </pre>
 */
public class AvroFastRecord implements GenericContainer, IndexedRecord {

  private static ByteSequence EMPTY_SEQUENCE = new ArrayByteSequence(new byte[0]);

  /**
   * The Avro schema.
   */
  private Schema schema;

  /**
   * The data array.
   */
  private Object[] values;

  /**
   * The nested records.
   */
  private AvroFastRecord[] nestedFields;

  /**
   * The primitive field indices for fast clearing.
   */
  private int[] primitiveFields;

  public AvroFastRecord(Schema schema) {
    this.schema = schema;
    this.values = new Object[schema.getFields().size()];

    // find all nested nested fields
    this.nestedFields = schema.getFields().stream().filter(f -> f.schema().getType() == Type.RECORD).map(f -> {
      AvroFastRecord rec = new AvroFastRecord(f.schema());
      this.values[f.pos()] = rec;
      return rec;
    }).toArray(AvroFastRecord[]::new);

    // find all primitive fields
    this.primitiveFields = schema.getFields().stream().filter(f -> f.schema().getType() != Type.RECORD)
        .mapToInt(Field::pos).toArray();
  }

  /**
   * Clears all primitive fields (including nested record once).
   */
  public void clear() {
    for (int idx : this.primitiveFields)
      this.values[idx] = null;

    for (AvroFastRecord rec : this.nestedFields)
      rec.clear();
  }

  @Override
  public void put(int i, Object v) {
    this.values[i] = v;
  }

  @Override
  public Object get(int i) {
    return this.values[i];
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  /**
   * Create the core lookup map for column family/column qualifier. The leave
   * nodes are consumers that know which record/field to target.
   * 
   * @param rootRecord the root Avro record.
   * @return the lookup map.
   */
  public static Map<ByteSequence, Map<ByteSequence, RowBuilderCellConsumer>> createCellToFieldMap(
      AvroFastRecord rootRecord) {
    Map<ByteSequence, Map<ByteSequence, RowBuilderCellConsumer>> map = new HashMap<>();

    // setup GenericRecordBuilder for each column family
    for (Field field : rootRecord.getSchema().getFields()) {
      Schema nestedSchema = field.schema();

      ByteSequence columnFamily = new ArrayByteSequence(field.name());

      // top-level field
      if (nestedSchema.getType() != Type.RECORD) {
        // Map.of(...) in older JDK
        Map<ByteSequence, RowBuilderCellConsumer> subMap = new HashMap<>();
        subMap.put(EMPTY_SEQUENCE, createAvroCellConsumer(rootRecord, field));

        map.put(columnFamily, subMap);

        continue;
      }

      // nested fields
      Map<ByteSequence, RowBuilderCellConsumer> nestedLookupMap = nestedSchema.getFields().stream()
          .collect(Collectors.toMap(
              // nested name as key
              nestedField -> new ArrayByteSequence(nestedField.name()),
              // assign cells to field in nested record
              nestedField -> createAvroCellConsumer((AvroFastRecord) rootRecord.get(field.pos()), nestedField)));

      map.put(columnFamily, nestedLookupMap);
    }

    return map;
  }

  /**
   * Creates a consumer of cells that copy the data into the corresponding Avro
   * record fields.
   * 
   * @param record The record to populate.
   * @param field  The field to populate
   * @return The closure holding things together.
   */
  private static RowBuilderCellConsumer createAvroCellConsumer(AvroFastRecord record, Field field) {
    int pos = field.pos();

    if (field.schema().getType() == Type.STRING)
      // avoid byte array copying
      return (key, value) -> record.put(pos, new AvroUtf8Wrapper(value.get()));

    // get the fitting encoder
    Encoder<?> encoder = RowBuilderType.valueOf(field.getProp(AvroSchemaBuilder.ROWBUILDERTYPE_PROP)).getEncoder();
    return (key, value) -> record.put(pos, encoder.decode(value.get()));
  }
}
