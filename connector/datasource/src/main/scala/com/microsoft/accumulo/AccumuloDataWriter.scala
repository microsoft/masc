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

import org.apache.accumulo.core.client.Accumulo
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.client.lexicoder._
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.types.UTF8String 

class AccumuloDataWriter (tableName: String, schema: StructType, mode: SaveMode, properties: java.util.Properties)
  extends DataWriter[InternalRow] {

    // val context = new ClientContext(properties)
    // TODO: construct BatchWriterConfig from properties if passed in
    // val batchWriter = new TabletServerBatchWriter(context, new BatchWriterConfig)
    // private val tableId = Tables.getTableId(context, tableName)

    private val rowKeyIdx = schema.fieldIndex(properties.getProperty("rowkey"))

    private val client = Accumulo.newClient().from(properties).build()
    
    // TODO: new BatchWriterConfig().setMaxWriteThreads(numThreads).setMaxMemory(batchMemory)
    private val batchWriter = client.createBatchWriter(tableName)

    private val doubleEncoder = new DoubleLexicoder
    private val floatEncoder = new FloatLexicoder
    private val longEncoder = new LongLexicoder
    private val intEncoder = new IntegerLexicoder
    private val stringEncoder = new StringLexicoder

    private val doubleAccessor = InternalRow.getAccessor(DoubleType)
    private val floatAccessor = InternalRow.getAccessor(FloatType)
    private val longAccessor = InternalRow.getAccessor(LongType)
    private val intAccessor = InternalRow.getAccessor(IntegerType)
    private val stringAccessor = InternalRow.getAccessor(StringType)

    private def getEncoder(fieldIdx: Int, field: StructField) = {
        field.dataType match {
            case DoubleType => (record: InternalRow) => doubleEncoder.encode(doubleAccessor(record, fieldIdx).asInstanceOf[Double])
            case FloatType => (record: InternalRow) => floatEncoder.encode(floatAccessor(record, fieldIdx).asInstanceOf[Float])
            case LongType => (record: InternalRow) => longEncoder.encode(longAccessor(record, fieldIdx).asInstanceOf[Long])
            case IntegerType => (record: InternalRow) => intEncoder.encode(intAccessor(record, fieldIdx).asInstanceOf[Integer])
            case StringType => (record: InternalRow) => stringAccessor(record, fieldIdx).asInstanceOf[UTF8String].getBytes
        }
    }

    private val structAccessor = InternalRow.getAccessor(new StructType())

    // pre-compute which fields and how to create the mutations...
    private val recordToMutation = schema.fields.zipWithIndex
        // exclude rowkey
        .filter({ case (_, cfIdx: Int) => cfIdx != rowKeyIdx }) 
        // loop through the rest of the fields
        .map { case (cf: StructField, cfIdx: Int) => {
                // check which types we have top-level
                cf.dataType match {
                   case ct: StructType => {
                        val nestedFields = ct.fields.zipWithIndex.map {
                                case (cq: StructField, cqIdx) => {
                                    val cfBytes = stringEncoder.encode(cf.name)
                                    val cqBytes = stringEncoder.encode(cq.name)
                                    val encoder = getEncoder(cqIdx, cq)

                                    (rowKey: Array[Byte], nestedRecord: InternalRow) => {
                                        // not using the fluent interface to provide backward compat
                                        val mutation = new Mutation(rowKey)
                                        mutation.put(cfBytes, cqBytes, encoder(nestedRecord))
                                        batchWriter.addMutation(mutation)
                                    }
                                }
                            }

                        // parent function
                        (rowKey: Array[Byte], record: InternalRow) => {
                            val nestedRecord = structAccessor(record, cfIdx).asInstanceOf[InternalRow]

                            nestedFields.foreach { _(rowKey, nestedRecord) }
                        }
                   }
                   case _ => { 
                        val cfBytes = stringEncoder.encode(cf.name)
                        val encoder = getEncoder(cfIdx, cf)

                        (rowKey: Array[Byte], record: InternalRow) => {
                            // println(s"\twriting row ${cf.name}")

                            // not using the fluent interface to provide backward compat
                            val mutation = new Mutation(rowKey)
                            mutation.put(cfBytes, Array.empty[Byte], encoder(record))
                            batchWriter.addMutation(mutation)
                        }
                   }
                }
            }
        }

    // TODO: expose this as another input column
    // private val columnVisibilityEmpty = new ColumnVisibility

    def write(record: InternalRow): Unit = {
        // println(s"writing record: ${record}")

        val rowKey = stringAccessor(record, rowKeyIdx).asInstanceOf[UTF8String].getBytes
        recordToMutation.foreach { _(rowKey, record) }
    }

    def commit(): WriterCommitMessage = {
        batchWriter.close

        client.close
        WriteSucceeded
    }

    def abort(): Unit = {
        batchWriter.close
        client.close
    }

    object WriteSucceeded extends WriterCommitMessage
}
