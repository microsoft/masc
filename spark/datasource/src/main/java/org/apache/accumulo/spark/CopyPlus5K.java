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
package org.apache.accumulo.spark;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.*;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CopyPlus5K {

  public static class AccumuloRangePartitioner extends Partitioner {

    private static final long serialVersionUID = 1L;
    private List<String> splits;

    AccumuloRangePartitioner(String... listSplits) {
      this.splits = Arrays.asList(listSplits);
    }

    @Override
    public int getPartition(Object o) {
      int index = Collections.binarySearch(splits, ((Key) o).getRow().toString());
      index = index < 0 ? (index + 1) * -1 : index;
      return index;
    }

    @Override
    public int numPartitions() {
      return splits.size() + 1;
    }
  }

  private static void cleanupAndCreateTables(Properties props) throws Exception {
    FileSystem hdfs = FileSystem.get(new Configuration());
    if (hdfs.exists(rootPath)) {
      hdfs.delete(rootPath, true);
    }
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      if (client.tableOperations().exists(inputTable)) {
        client.tableOperations().delete(inputTable);
      }
      if (client.tableOperations().exists(outputTable)) {
        client.tableOperations().delete(outputTable);
      }
      // Create tables
      client.tableOperations().create(inputTable);
      client.tableOperations().create(outputTable);

      // Write data to input table
      try (BatchWriter bw = client.createBatchWriter(inputTable)) {
        for (int i = 0; i < 100; i++) {
          Mutation m = new Mutation(String.format("%03d", i));
          m.at().family("cf1").qualifier("cq1").put("" + i);
          bw.addMutation(m);
        }
      }
    }
  }

  private static String getPid() throws IOException {
    byte[] bo = new byte[256];
    InputStream is = new FileInputStream("/proc/self/stat");
    is.read(bo);
    for (int i = 0; i < bo.length; i++) {
      if ((bo[i] < '0') || (bo[i] > '9')) {
        return new String(bo, 0, i);
      }
    }
    is.close();
    return "-1";
    // CLibrary.INSTANCE.getpid()
    // ProcessHandle.current().pid()
  }

  private static final String inputTable = "spark_example_input";
  private static final String outputTable = "spark_example_output";
  private static final Path rootPath = new Path("/spark_example/");

  public static void main(String[] args) throws Exception {

    LogManager.getLogger("org.apache.accumulo.core.client.mapred").setLevel(Level.ALL);

    Runtime.getRuntime()
        .exec(new String[] {"bash", "-c", "ps aux | grep " + getPid() + " > /tmp/master.log"})
        .waitFor();

    if ((!args[0].equals("batch") && !args[0].equals("bulk")) || args[1].isEmpty()) {
      System.out.println("Usage: ./run.sh [batch|bulk] /path/to/accumulo-client.properties");
      System.exit(1);
    }

    // Read client properties from file
    final Properties props = Accumulo.newClientProperties().from(args[1]).build();

    cleanupAndCreateTables(props);

    SparkConf conf = new SparkConf();
    conf.setAppName("CopyPlus5K");
    // // KryoSerializer is needed for serializing Accumulo Key when partitioning
    // data
    // // for bulk import
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(new Class[] {Key.class, Value.class, Properties.class});

    // JavaSparkContext sc = new JavaSparkContext(conf);
    // JavaSqlSparkContext
    SparkSession sc = SparkSession.builder().config(conf).getOrCreate();

    HashMap<String,String> propMap = new HashMap<>();
    for (final String name : props.stringPropertyNames())
      propMap.put(name, props.getProperty(name));

    propMap.put("table", inputTable);

    // TODO: don't like the formatting...
    StructType schema = new StructType(
        new StructField[] {new StructField("f1", DataTypes.StringType, true,
            new MetadataBuilder().putString("cf", "cf1").putString("cq", "cq1").build())});

    Dataset<Row> df = sc.read().format("org.apache.accumulo.spark").options(propMap).schema(schema)
        .load();
    df.show(10);

    /*
     * Job job = Job.getInstance();
     * 
     * IteratorSetting avroIterator = new IteratorSetting(20, "AVRO", AvroRowEncoderIterator.class);
     * String json =
     * "{\"rowKeyTargetColumn\":\"rowKey\",\"mapping\":{\"f1\":{\"columnFamily\":\"cf1\",\"columnQualifier\":\"cq1\",\"type\":\"STRING\"}}}";
     * avroIterator.addOption("schema", json);
     * 
     * // Read input from Accumulo
     * AccumuloInputFormat.configure().clientProperties(props).table(inputTable) //
     * .localIterators(false) .addIterator(avroIterator).store(job);
     * 
     * JavaRDD<Tuple2<Key, Value>> data = sc.sparkContext() .newAPIHadoopRDD(job.getConfiguration(),
     * AccumuloInputFormat.class, Key.class, Value.class).toJavaRDD();
     * 
     * JavaRDD<Row> dataRows = data.map(t -> { List<Schema.Field> fields = Arrays.asList(new
     * Schema.Field("f1", Schema.create(Schema.Type.STRING), null, null));
     * 
     * Schema schema = Schema.createRecord(fields);
     * 
     * // TODO: we should not encode the row key on the Accumulo side, but rather here // to avoid
     * duplication byte[] byteData = t._2().get();
     * 
     * DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema); BinaryDecoder decoder
     * = DecoderFactory.get().binaryDecoder(byteData, null);
     * 
     * GenericRecord rec1 = new GenericData.Record(schema);
     * 
     * reader.read(rec1, decoder);
     * 
     * Utf8 str = (Utf8) rec1.get("f1"); // maybe there is an optimized version from byte[] to Spark
     * Row String return RowFactory.create(str.toString()); });
     * 
     * StructType schema = new StructType( new StructField[] { new StructField("f1",
     * DataTypes.StringType, false, Metadata.empty()) // new StructField("cost",
     * DataTypes.IntegerType, false, Metadata.empty()) });
     * 
     * Dataset<Row> df = sc.createDataFrame(dataRows, schema);
     * 
     * df.show(10);
     */
  }
}
