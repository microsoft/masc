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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.sources.Filter
import scala.collection.JavaConverters._
import org.apache.log4j.Logger

// TODO: https://github.com/apache/spark/blob/053dd858d38e6107bc71e0aa3a4954291b74f8c8/sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/SupportsReportPartitioning.java
// in head of spark github repo
// import org.apache.spark.sql.connector.read.{SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.v2.reader.{SupportsPushDownFilters, SupportsPushDownRequiredColumns}

import org.apache.hadoop.io.Text

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(1L)
class AccumuloDataSourceReader(schema: StructType, options: DataSourceOptions)
  extends DataSourceReader with Serializable with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
  private val logger = Logger.getLogger(classOf[AccumuloDataSourceReader])

  private val defaultMaxPartitions = 200

  var filters = Array.empty[Filter]

  val rowKeyColumn = options.get("rowkey").orElse("rowkey")

  private val baseSchema = StructType(schema.add(rowKeyColumn, DataTypes.StringType, nullable = true).fields)

  // needs to be nullable so that Avro doesn't barf when we want to add another column
  // add any output fields we find in a mleap pipeline
  private var mleapFields = MLeapUtil.mleapSchemaToCatalyst(options.get("mleap").orElse(""))
  private var requiredSchema = StructType(baseSchema ++ mleapFields)

  private var filterInJuel: Option[String] = None

  // SupportsPushDownRequiredColumns implementation
  override def pruneColumns(requiredSchema: StructType): Unit = {
      this.requiredSchema = requiredSchema
  }

  private def jsonSchema() = {
    val schemaWithoutRowKey = new StructType(requiredSchema.fields.filter(_.name != rowKeyColumn))
  
    AvroUtil.catalystSchemaToJson(schemaWithoutRowKey)
  }

  def readSchema: StructType = requiredSchema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // unfortunately predicates on nested elements are not pushed down by Spark
    // https://issues.apache.org/jira/browse/SPARK-17636
    // https://github.com/apache/spark/pull/22535

    val result = new FilterToJuel(jsonSchema.attributeToVariableMapping, rowKeyColumn)
      .serializeFilters(filters, options.get("filter").orElse(""))

    this.filters = result.supportedFilters.toArray

    if (result.serializedFilter.length > 0) {
      this.filterInJuel = Some("${" + result.serializedFilter + "}")
      logger.info(s"JUEL filter: ${this.filterInJuel}")
    }

    result.unsupportedFilters.toArray
  }

  override def pushedFilters(): Array[Filter] = filters

  def planInputPartitions: java.util.List[InputPartition[InternalRow]] = {
    val tableName = options.tableName.get
    val maxPartitions = options.getInt("maxPartitions", defaultMaxPartitions) - 1
    val properties = new java.util.Properties()
    // can use .putAll(options.asMap()) due to https://github.com/scala/bug/issues/10418
    options.asMap.asScala.foreach { case (k, v) => properties.setProperty(k, v) }

    val splits = ArrayBuffer(Array.empty[Byte], Array.empty[Byte])

    val client = Accumulo.newClient().from(properties).build()
    val tableSplits = client.tableOperations().listSplits(tableName, maxPartitions) 
    client.close()

    splits.insertAll(1, tableSplits.asScala.map(_.getBytes))

    logger.info(s"Splits '${tableSplits}'")

    new java.util.ArrayList[InputPartition[InternalRow]](
      (1 until splits.length).map(i =>
        new PartitionReaderFactory(tableName, splits(i - 1), splits(i),
          baseSchema, mleapFields, properties, rowKeyColumn,
          jsonSchema.json, filterInJuel)
      ).asJava
    )
  }
}

class PartitionReaderFactory(tableName: String,
                             start: Array[Byte],
                             stop: Array[Byte],
                             schema: StructType,
                             mleapFields: Seq[StructField],
                             properties: java.util.Properties,
                             rowKeyColumn: String,
                             jsonSchema: String,
                             filterInJuel: Option[String])
  extends InputPartition[InternalRow] {

  def createPartitionReader: InputPartitionReader[InternalRow] = {
    val startText = if (start.length == 0) "-inf" else s"'${new Text(start)}'"
    val stopText = if (stop.length == 0) "inf" else s"'${new Text(stop)}'"

    Logger.getLogger(classOf[AccumuloDataSourceReader]).info(s"Partition reader for ${startText} to ${stopText}")

    new AccumuloInputPartitionReader(tableName, start, stop, schema, mleapFields, properties, rowKeyColumn, jsonSchema, filterInJuel)
  }
}
