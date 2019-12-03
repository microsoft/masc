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

import java.io.ByteArrayOutputStream

import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.junit.runner.RunWith

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VerifyAccumuloSchema extends FunSuite {
  test("Validate catalyst schema to json serialization") {
    val schema = (new StructType)
       .add(StructField("cf1", (new StructType)
          .add("cq1", DataTypes.StringType, true)
          .add("cq2", DataTypes.DoubleType, true)
          , true))
      .add(StructField("cf2", (new StructType)
        .add("cq_a", DataTypes.IntegerType, true)
        .add("cq_b", DataTypes.FloatType, true)
        , true))
      .add("cf3", DataTypes.StringType, false)

    val jsonActual = AvroUtil.catalystSchemaToJson(schema).json
    val jsonExpected = "[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"fvn\":\"v0\",\"t\":\"STRING\",\"o\":true}" +
      ",{\"cf\":\"cf1\",\"cq\":\"cq2\",\"fvn\":\"v1\",\"t\":\"DOUBLE\",\"o\":true}" +
      ",{\"cf\":\"cf2\",\"cq\":\"cq_a\",\"fvn\":\"v2\",\"t\":\"INTEGER\",\"o\":true}" +
      ",{\"cf\":\"cf2\",\"cq\":\"cq_b\",\"fvn\":\"v3\",\"t\":\"FLOAT\",\"o\":true}" +
      ",{\"cf\":\"cf3\",\"fvn\":\"v4\",\"t\":\"STRING\",\"o\":true}]"

    assert(jsonActual == jsonExpected)
  }

  test("Validate catalyst schema to json serialization with pruned output schema") {
    val inputSchema = (new StructType)
      .add(StructField("cf1", (new StructType)
        .add("cq1", DataTypes.StringType, true)
        .add("cq2", DataTypes.DoubleType, true)
        , true))
      .add(StructField("cf2", (new StructType)
        .add("cq_a", DataTypes.IntegerType, true)
        .add("cq_b", DataTypes.FloatType, true)
        , true))
      .add("cf3", DataTypes.StringType, false)
      .add("cf4", DataTypes.LongType, false)

    val outputSchema = (new StructType)
      .add(StructField("cf1", (new StructType)
        .add("cq1", DataTypes.StringType, true)
        , true))
      .add("cf3", DataTypes.StringType, false)

    val jsonActual = AvroUtil.catalystSchemaToJson(inputSchema, outputSchema).json
    val jsonExpected = "[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"fvn\":\"v0\",\"t\":\"STRING\",\"o\":true}" +
      ",{\"cf\":\"cf1\",\"cq\":\"cq2\",\"fvn\":\"v1\",\"t\":\"DOUBLE\",\"o\":false}" +
      ",{\"cf\":\"cf2\",\"cq\":\"cq_a\",\"fvn\":\"v2\",\"t\":\"INTEGER\",\"o\":false}" +
      ",{\"cf\":\"cf2\",\"cq\":\"cq_b\",\"fvn\":\"v3\",\"t\":\"FLOAT\",\"o\":false}" +
      ",{\"cf\":\"cf3\",\"fvn\":\"v4\",\"t\":\"STRING\",\"o\":true}" +
      ",{\"cf\":\"cf4\",\"fvn\":\"v5\",\"t\":\"LONG\",\"o\":false}]"

    assert(jsonActual == jsonExpected)
  }

  test("Validate catalyst schema to avro serialization") {
    val schema = (new StructType)
      .add(StructField("cf1", (new StructType)
        .add("cq1", DataTypes.StringType, true)
        .add("cq2", DataTypes.DoubleType, false)
        .add("cq3", DataTypes.DoubleType, true)
        , true))
      .add(StructField("cf2", (new StructType)
        .add("cq_a", DataTypes.IntegerType, true)
        .add("cq_b", DataTypes.FloatType, true)
        , true))

    val avroSchema = AvroUtil.catalystSchemaToAvroSchema(schema)

    val builder = new GenericRecordBuilder(avroSchema)

    val builderCf1 = new GenericRecordBuilder(avroSchema.getField("cf1").schema())
    val builderCf2 = new GenericRecordBuilder(avroSchema.getField("cf2").schema())
    // check if clear() helps perf?

    builderCf1.set("cq1", "foo")
    builderCf1.set("cq2", 2.3)

    builderCf2.set("cq_a", 1)
    builderCf2.set("cq_b", 1.2f)

    builder.set("cf1", builderCf1.build())
    builder.set("cf2", builderCf2.build())

    val output = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.jsonEncoder(avroSchema, output)

    val writer = new SpecificDatumWriter[GenericRecord](avroSchema)
    writer.write(builder.build(), encoder)

    encoder.flush()

    val jsonActual = new String(output.toByteArray)

    val jsonExpected = "{\"cf1\":{\"cq1\":{\"string\":\"foo\"}," +
      "\"cq2\":2.3,\"cq3\":null}," +
      "\"cf2\":{\"cq_a\":{\"int\":1},\"cq_b\":{\"float\":1.2}}}"

    assert(jsonActual == jsonExpected)
  }

  test("Validate unsupported types") {
    val schema = (new StructType)
      .add("cf3", DataTypes.CalendarIntervalType, false)

    assertThrows[UnsupportedOperationException] {
      AvroUtil.catalystSchemaToAvroSchema(schema)
    }
  }
}