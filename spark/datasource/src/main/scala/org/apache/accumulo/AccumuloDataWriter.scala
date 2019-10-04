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
    // create table if it's not there
    if (!client.tableOperations.exists(tableName)) 
        client.tableOperations.create(tableName)
    
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

    private def encode(record: InternalRow, fieldIdx: Int, field: StructField) = {
        field.dataType match {
            case DoubleType => doubleEncoder.encode(doubleAccessor(record, fieldIdx).asInstanceOf[Double])
            case FloatType => floatEncoder.encode(floatAccessor(record, fieldIdx).asInstanceOf[Float])
            case LongType => longEncoder.encode(longAccessor(record, fieldIdx).asInstanceOf[Long])
            case IntegerType => intEncoder.encode(intAccessor(record, fieldIdx).asInstanceOf[Integer])
            case StringType => stringAccessor(record, fieldIdx).asInstanceOf[UTF8String].getBytes
        }
    }

    private val structAccessor = InternalRow.getAccessor(new StructType())

    // TODO: expose this as another input column
    // private val columnVisibilityEmpty = new ColumnVisibility

    def write(record: InternalRow): Unit = {
        // println(s"writing record: ${record}")

        // TODO: iterating over the schema should be done outside of the write-loop
        schema.fields.zipWithIndex.foreach {
            // loop through fields
            case (cf: StructField, cfIdx: Int) => {
                if (cfIdx != rowKeyIdx) {
                    // check which types we have top-level
                    cf.dataType match {
                       case ct: StructType => {
                            val nestedRecord = structAccessor(record, cfIdx).asInstanceOf[InternalRow]

                            ct.fields.zipWithIndex.foreach {
                                case (cq: StructField, cqIdx) => {
                                    val mutation = new Mutation(stringAccessor(record, rowKeyIdx).asInstanceOf[UTF8String].getBytes)
                                    mutation.put(stringEncoder.encode(cf.name), stringEncoder.encode(cq.name), encode(nestedRecord, cqIdx, cq))
                                    batchWriter.addMutation(mutation)
                                    batchWriter.flush
                                }
                            }
                       }
                       case _ => { 
                           val bytes = encode(record, cfIdx, cf) 
                        //    println(s"\twriting row ${cf.name} with bytes: ${bytes.length}")

                           val mutation = new Mutation(stringAccessor(record, rowKeyIdx).asInstanceOf[UTF8String].getBytes)
                           mutation.put(stringEncoder.encode(cf.name), Array.empty[Byte], encode(record, cfIdx, cf))
                           batchWriter.addMutation(mutation)
                        //    batchWriter.flush
                       }
                    }
                }
            }
        }

        // batchWriter.flush
    }

    def commit(): WriterCommitMessage = {
        // println("MARKUS COMMIT")
        // batchWriter.flush()
        batchWriter.close

        client.close
        WriteSucceeded
    }

    def abort(): Unit = {
        // println("MARKUS ABORT")
        batchWriter.close
        client.close
    }

    object WriteSucceeded extends WriterCommitMessage
}
