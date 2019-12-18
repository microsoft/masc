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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.accumulo.core.client.Accumulo
import org.apache.hadoop.io.Text
import scala.collection.JavaConverters._
import org.apache.log4j.Logger

class AccumuloDataSourceWriter(schema: StructType, mode: SaveMode, options: DataSourceOptions)
  extends DataSourceWriter {
  
  private val logger = Logger.getLogger(classOf[AccumuloDataSourceWriter])

  val tableName = options.tableName.get
  val properties = new java.util.Properties()
  // cannot use .putAll(options.asMap()) due to https://github.com/scala/bug/issues/10418
  options.asMap.asScala.foreach { case (k, v) => properties.setProperty(k, v) }

  // defaults based on https://accumulo.apache.org/docs/2.x/configuration/client-properties
  val batchThread = options.get("batchThread").orElse("3").toInt
  val batchMemory = options.get("batchMemory").orElse("50000000").toLong

  val client = Accumulo.newClient().from(properties).build()
  // create table if it's not there
  if (!client.tableOperations.exists(tableName)) {
      // adding splits to a newly created table
      val splits = new java.util.TreeSet(
          properties.getProperty("splits", "")
              .split(",")
              .map(new Text(_))
              .toSeq
              .asJava)
          
      logger.info(s"Creating table with splits: ${splits}")

      client.tableOperations.create(tableName)

      if (!splits.isEmpty) {
          client.tableOperations.addSplits(tableName, splits)
      }
  }

  client.close

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new AccumuloDataWriterFactory(tableName, schema, mode, properties, batchThread, batchMemory)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }
}

class AccumuloDataWriterFactory(tableName: String, schema: StructType, mode: SaveMode, properties: java.util.Properties, batchThread: Int, batchMemory: Long)
  extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new AccumuloDataWriter(tableName, schema, mode, properties, batchThread, batchMemory)
  }
}
