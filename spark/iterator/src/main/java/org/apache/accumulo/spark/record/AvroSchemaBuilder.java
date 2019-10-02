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

package org.apache.accumulo.spark.record;

import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Builds the AVRO Schema from the user-supplied JSON encoded schema.
 */
public class AvroSchemaBuilder {
  public static final String ROWBUILDERTYPE_PROP = "rowBuilderType";

  private static SchemaBuilder.FieldAssembler<Schema> addAvroField(SchemaBuilder.FieldAssembler<Schema> builder,
      RowBuilderField field, String name) {

    RowBuilderType type = field.getRowBuilderType();

    SchemaBuilder.FieldBuilder<Schema> fieldBuilder = builder
        // configure the field name
        .name(name);

    // pass in alias
    if (field.getFilterVariableName() != null && field.getFilterVariableName().length() > 0)
      fieldBuilder = fieldBuilder.aliases(field.getFilterVariableName());

    SchemaBuilder.BaseTypeBuilder<SchemaBuilder.FieldAssembler<Schema>> intermediate = fieldBuilder
        // encode rowBuilderType so we can only operator on schema
        .prop(ROWBUILDERTYPE_PROP, type.name())
        // all fields are optional
        .type().optional();

    switch (type) {
      case String:
        return intermediate.stringType();
      case Long:
        return intermediate.longType();
      case Integer:
        return intermediate.intType();
      case Double:
        return intermediate.doubleType();
      case Float:
        return intermediate.floatType();
      case Boolean:
        return intermediate.booleanType();
      case Bytes:
        return intermediate.bytesType();
      default:
        throw new IllegalArgumentException("Unsupported type '" + type + "'");
    }
  }

  private static SchemaBuilder.FieldAssembler<Schema> closeFieldAssembler(
      SchemaBuilder.FieldAssembler<Schema> rootAssembler, SchemaBuilder.FieldAssembler<Schema> columnFieldsAssembler,
      String columnFamily) {

    if (columnFieldsAssembler == null)
      return rootAssembler;

    // add nested type to to root assembler
    return rootAssembler.name(columnFamily).type(columnFieldsAssembler.endRecord()).noDefault();
  }

  public static Schema buildSchema(Collection<RowBuilderField> schemaFields) {
    // construct schema
    SchemaBuilder.FieldAssembler<Schema> rootAssembler = SchemaBuilder.record("root").fields();

    // note that the order needs to be exactly in-sync with the avro schema
    // generated on the MMLSpark/Scala side
    String lastColumnFamily = null;
    SchemaBuilder.FieldAssembler<Schema> columnFieldsAssembler = null;
    for (RowBuilderField schemaField : schemaFields) {

      String columnFamily = schemaField.getColumnFamily();
      String columnQualifier = schemaField.getColumnQualifier();

      if (columnQualifier != null) {
        if (lastColumnFamily == null || !lastColumnFamily.equals(columnFamily)) {

          // close previous record
          rootAssembler = closeFieldAssembler(rootAssembler, columnFieldsAssembler, lastColumnFamily);

          // open new record
          columnFieldsAssembler = SchemaBuilder.record(columnFamily).fields();
        }

        // add the current field
        columnFieldsAssembler = addAvroField(columnFieldsAssembler, schemaField, columnQualifier);
      } else {
        // close previous record
        rootAssembler = closeFieldAssembler(rootAssembler, columnFieldsAssembler, lastColumnFamily);
        columnFieldsAssembler = null;

        // add the top-level field
        rootAssembler = addAvroField(rootAssembler, schemaField, columnFamily);
      }

      lastColumnFamily = columnFamily;

    }

    rootAssembler = closeFieldAssembler(rootAssembler, columnFieldsAssembler, lastColumnFamily);

    // setup serialization
    return rootAssembler.endRecord();
  }
}
