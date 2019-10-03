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

package org.apache.accumulo

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.Accumulo
import org.apache.accumulo.core.data.{Key, Range}
import org.apache.accumulo.core.security.Authorizations
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.io.Text
import java.io.IOException
import java.util.Collections
import org.apache.spark.unsafe.types.UTF8String

@SerialVersionUID(1L)
class AccumuloInputPartitionReader(tableName: String,
                                   start: Array[Byte],
                                   stop: Array[Byte],
                                   schema: StructType,
                                   properties: java.util.Properties,
                                   rowKeyColumn: String,
                                   jsonSchema: String,
                                   filterInJuel: Option[String])
  extends InputPartitionReader[InternalRow] with Serializable {

  val defaultPriority = "20"
  val defaultNumQueryThreads = "1"

  private val priority = Integer.valueOf(properties.getProperty("priority", defaultPriority))
  // this parameter is impacted by number of accumulo splits and spark partitions and executors
  private val numQueryThreads = Integer.valueOf(properties.getProperty("numQueryThreads", defaultNumQueryThreads))

  private val authorizations = new Authorizations()
  private val client = Accumulo.newClient().from(properties).build()
  private val scanner = client.createBatchScanner(tableName, authorizations, numQueryThreads)
  scanner.setRanges(Collections.singletonList(
    new Range(new Key(start), false, new Key(stop), true))
  )

  private val avroIterator = new IteratorSetting(
    priority,
    "AVRO",
    "org.apache.accumulo.spark.AvroRowEncoderIterator")

  // drop rowKey from schema
  private val schemaWithoutRowKey = new StructType(schema.fields.filter(_.name != rowKeyColumn))
  private val schemaWithRowKey = schema

  private val rowKeyColumnIndex = {
    if (schema.fieldNames.contains(rowKeyColumn))
      schema.fieldIndex(rowKeyColumn)
    else
      -1
  }

  // AVRO Iterator setup
  avroIterator.addOption("schema", jsonSchema)
  if (filterInJuel.isDefined)
    avroIterator.addOption("filter", filterInJuel.get)

  scanner.addScanIterator(avroIterator)

  // TODO: support additional user-supplied iterators
  private val scannerIterator = scanner.iterator()

  // filter out row-key target from schema generation
  // populate it
  private val avroSchema = AvroUtil.catalystSchemaToAvroSchema(schemaWithoutRowKey)
  private val deserializer = new AvroDeserializer(avroSchema, schemaWithRowKey)
  private val reader = new SpecificDatumReader[GenericRecord](avroSchema)

  private var decoder: BinaryDecoder = _
  private var currentRow: InternalRow = _
  private var datum: GenericRecord = _

  private val rowKeyText = new Text()

  override def close(): Unit = {
    if (scanner != null)
      scanner.close()

    if (client != null)
      client.close()
  }

  @IOException
  override def next: Boolean = {
    if (scannerIterator.hasNext) {
      val entry = scannerIterator.next
      val data = entry.getValue.get

      // byte[] -> avro
      decoder = DecoderFactory.get.binaryDecoder(data, decoder)
      datum = reader.read(datum, decoder)

      // avro -> catalyst
      currentRow = deserializer.deserialize(datum).asInstanceOf[InternalRow]

      if (rowKeyColumnIndex >= 0) {
        // move row key id into internalrow
        entry.getKey.getRow(rowKeyText)

        // avoid yet another byte array copy...
        val str = UTF8String.fromBytes(rowKeyText.getBytes, 0, rowKeyText.getLength)
        currentRow.update(rowKeyColumnIndex, str)
      }

      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow
}