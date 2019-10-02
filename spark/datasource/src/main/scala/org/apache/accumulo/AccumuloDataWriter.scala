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
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{StructField, StructType}

class AccumuloDataWriter (tableName: String, schema: StructType, mode: SaveMode, properties: java.util.Properties)
  extends DataWriter[InternalRow] {

    // val context = new ClientContext(properties)
    // TODO: construct BatchWriterConfig from properties if passed in
    // val batchWriter = new TabletServerBatchWriter(context, new BatchWriterConfig)
    // private val tableId = Tables.getTableId(context, tableName)

    private val client = Accumulo.newClient().from(properties).build();
    private val batchWriter = client.createBatchWriter(tableName)

    def write(record: InternalRow): Unit = {
        schema.fields.zipWithIndex.foreach {
            case (cf: StructField, structIdx: Int) =>
                cf.dataType match {
                    case struct: StructType =>
                        struct.fields.zipWithIndex.foreach {
                            case (cq: StructField, fieldIdx: Int) =>
                                val recordStruct = record.getStruct(structIdx, struct.size)
                                // FIXME: put in correct row id
                                batchWriter.addMutation(new Mutation(new Text("row_id"))
                                  .at()
                                  .family(cf.name)
                                  .qualifier(cq.name)
                                  .put(new Text(recordStruct.getString(fieldIdx)))
                                )
                        }
                }
        }
    }

    def commit(): WriterCommitMessage = {
        batchWriter.flush()
        batchWriter.close()
        WriteSucceeded
    }

    def abort(): Unit = {
        batchWriter.close()
    }

    object WriteSucceeded extends WriterCommitMessage
}
