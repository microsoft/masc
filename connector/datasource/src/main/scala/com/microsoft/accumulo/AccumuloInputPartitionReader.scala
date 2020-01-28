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

import java.io.IOException

import org.apache.accumulo.core.client.{Accumulo, IteratorSetting}
import org.apache.accumulo.core.data.{Key, Range}
import org.apache.accumulo.core.security.Authorizations
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConverters._

@SerialVersionUID(1L)
class AccumuloInputPartitionReader(tableName: String,
                                   ranges: Seq[Seq[Array[Byte]]],
                                   inputSchema: StructType,
                                   outputSchema: StructType,
                                   properties: java.util.Properties,
                                   rowKeyColumn: String,
                                   filterInJuel: Option[String])
  extends InputPartitionReader[InternalRow] with Serializable {

  private val logger = Logger.getLogger(classOf[AccumuloInputPartitionReader])

  val defaultPriority = "20"
  val defaultNumQueryThreads: String = math.min(16, ranges.length).toString

  private val priority = Integer.valueOf(properties.getProperty("priority", defaultPriority))
  // this parameter is impacted by number of accumulo splits and spark partitions and executors
  private val numQueryThreads = Integer.valueOf(properties.getProperty("numQueryThreads", defaultNumQueryThreads))

  private val authorizations = new Authorizations()
  private val client = Accumulo.newClient().from(properties).build()
  private val scanner = client.createBatchScanner(tableName, authorizations, numQueryThreads)

  private def createRange(start: Array[Byte], stop: Array[Byte]) =
      new Range(
        if (start.length == 0) null else new Key(start),
        start.length == 0, 
        if (stop.length == 0) null else new Key(stop),
        true)

  scanner.setRanges(ranges.map(t => createRange(t(0), t(1))).asJava)

  private val avroIterator = new IteratorSetting(
    priority,
    "AVRO",
    "com.microsoft.accumulo.spark.AvroRowEncoderIterator")

  // only fetch column families we care for (and don't filter for the mleapFields which are artificially added later)
  inputSchema.fields.foreach(f => scanner.fetchColumnFamily(f.name))

  private val rowKeyColumnIndex = {
    if (outputSchema.fieldNames.contains(rowKeyColumn))
      outputSchema.fieldIndex(rowKeyColumn)
    else
      -1
  }

  // AVRO Iterator setup
  val jsonSchema: String = AvroUtil.catalystSchemaToJson(inputSchema, outputSchema).json

  logger.info(s"JSON schema: $jsonSchema")
  avroIterator.addOption("schema", jsonSchema)
  if (filterInJuel.isDefined)
    avroIterator.addOption("filter", filterInJuel.get)

  // list of output columns
//  val prunedColumns = schema.map(_.name).mkString(",")
//  logger.info(s"Pruned columns: ${prunedColumns}")
//  avroIterator.addOption("prunedcolumns", prunedColumns)

  // forward options
  Seq("mleap", "mleapfilter", "mleapguid", "exceptionlogfile")
    .foreach { key => avroIterator.addOption(key, properties.getProperty(key, "")) }

  scanner.addScanIterator(avroIterator)

  // TODO: support additional user-supplied iterators
  private val scannerIterator = scanner.iterator()

  // filter out row-key target from schema generation
  private val schemaWithoutRowKey = new StructType(outputSchema.fields.filter(_.name != rowKeyColumn))

  // the serialized AVRO does not contain the row key as it comes with the key/value pair anyway
  private val avroSchema = AvroUtil.catalystSchemaToAvroSchema(schemaWithoutRowKey)

  // pass the schema for the avro input along with the target output schema (incl. row key)
  private val deserializer = new AvroDeserializer(avroSchema, outputSchema)
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