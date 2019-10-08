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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

class AccumuloDataSourceWriter(schema: StructType, mode: SaveMode, options: DataSourceOptions)
  extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    val tableName = options.tableName.get
    val properties = new java.util.Properties()
    // cannot use .putAll(options.asMap()) due to https://github.com/scala/bug/issues/10418
    options.asMap.asScala.foreach { case (k, v) => properties.setProperty(k, v) }

    new AccumuloDataWriterFactory(tableName, schema, mode, properties)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }
}

class AccumuloDataWriterFactory(tableName: String, schema: StructType, mode: SaveMode, properties: java.util.Properties)
  extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new AccumuloDataWriter(tableName, schema, mode, properties)
  }
}
