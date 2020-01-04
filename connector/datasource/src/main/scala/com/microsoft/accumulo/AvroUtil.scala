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

package com.microsoft.accumulo

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion

import scala.beans.BeanProperty

// keeping the property names short to not hit any limits
case class RowBuilderField(@BeanProperty cf: String,  // column family
                           @BeanProperty cq: String,  // column qualifier
                           @BeanProperty fvn: String, // filter variable name
                           @BeanProperty t: String,   // type
                           @BeanProperty o: Boolean   // output
                          )

case class JsonSchema(json: String, attributeToVariableMapping: Map[String, String])

@SerialVersionUID(1L)
object AvroUtil {
  def catalystSchemaToJson(inputSchema: StructType): JsonSchema = catalystSchemaToJson(inputSchema, inputSchema)

  def catalystSchemaToJson(inputSchema: StructType, outputSchema: StructType): JsonSchema = {

    var attributeToVariableMapping = scala.collection.mutable.Map[String,  String]()

    var i = 0
    val selectedFields = inputSchema.fields.flatMap(cf => {
      val outputField = outputSchema.find(f => f.name == cf.name)

      cf.dataType match {
        case cft: StructType => cft.fields.map(cq =>
          RowBuilderField(
            cf.name,
            cq.name,
            {
              val variableName = s"v$i"
              attributeToVariableMapping += (s"${cf.name}.${cq.name}" -> variableName)
              i += 1

              variableName
            },
            // TODO: toUpperCase() is weird...
            cq.dataType.typeName.toUpperCase,
            // either the column family is not need -> output = false
            // otherwise we need to check if the column qualifier is present in the output list
            if (outputField.isEmpty) false else outputField.get.dataType.asInstanceOf[StructType].exists(f => f.name == cq.name)
          )
        )
        case _: DataType => Seq(RowBuilderField(
          cf.name,
          null,
          {
            val variableName = s"v$i"
            attributeToVariableMapping += (s"${cf.name}" -> variableName)
            i += 1

            variableName
          },
          // TODO: toUpperCase() is weird...
          cf.dataType.typeName.toUpperCase,
          outputField.isDefined
        ))
      }
    })

    try {
      val mapper = new ObjectMapper()

      // disable serialization of null-values
      mapper.setSerializationInclusion(Inclusion.NON_NULL)

      JsonSchema(mapper.writeValueAsString(selectedFields), attributeToVariableMapping.toMap)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(e)
    }
  }

  implicit class CatalystSchemaToAvroRecordBuilder(builder: SchemaBuilder.FieldAssembler[Schema]) {
    def addAvroRecordField(field: StructField): SchemaBuilder.FieldAssembler[Schema] = {
      (field.dataType, field.nullable) match {
          case (DataTypes.BinaryType, true) => builder.optionalBytes(field.name)
          case (DataTypes.BinaryType, false) => builder.requiredBytes(field.name)
          case (DataTypes.BooleanType, true) => builder.optionalBoolean(field.name)
          case (DataTypes.BooleanType, false) => builder.requiredBoolean(field.name)
          case (DataTypes.DoubleType, true) => builder.optionalDouble(field.name)
          case (DataTypes.DoubleType, false) => builder.requiredDouble(field.name)
          case (DataTypes.FloatType, true) => builder.optionalFloat(field.name)
          case (DataTypes.FloatType, false) => builder.requiredFloat(field.name)
          case (DataTypes.IntegerType, true) => builder.optionalInt(field.name)
          case (DataTypes.IntegerType, false) => builder.requiredInt(field.name)
          case (DataTypes.LongType, true) => builder.optionalLong(field.name)
          case (DataTypes.LongType, false) => builder.requiredLong(field.name)
          case (DataTypes.StringType, true) => builder.optionalString(field.name)
          case (DataTypes.StringType, false) => builder.requiredString(field.name)
          // TODO: date/time support?
          case _ => throw new UnsupportedOperationException(s"Unsupported type: $field.dataType")
      }
    }

    def addAvroRecordFields(schema: StructType): SchemaBuilder.FieldAssembler[Schema] = {
      schema.fields.foldLeft(builder) { (builder, field) => builder.addAvroRecordField(field) }
    }
  }

  def catalystSchemaToAvroSchema(schema: StructType): Schema = {
    val fieldBuilder = SchemaBuilder.record("root")
      .fields()

    schema.fields.foldLeft(fieldBuilder) { (_, field) =>
        field.dataType match {
          // nested fields
          case cft: StructType =>
            fieldBuilder
              .name(field.name)
              .`type`(SchemaBuilder
                .record(field.name)
                .fields
                .addAvroRecordFields(cft)
                .endRecord())
              .noDefault()
          // top level fields
          case _ => fieldBuilder.addAvroRecordField(field)
        }
      }
      .endRecord()
  }
}
