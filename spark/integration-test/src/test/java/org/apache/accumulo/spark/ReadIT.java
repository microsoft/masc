package org.apache.accumulo.spark;

import java.io.File;
import java.util.Properties;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
//import org.apache.accumulo.client.AccumuloClient;
import org.junit.Test;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.HashMap;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ReadIT {

      @Test
      public void testDataReading() {
            File propsFile = new File("target/accumulo2-maven-plugin/spark-connector-instance");
            Properties props = MiniAccumuloCluster.getClientProperties(propsFile);
            // AccumuloClient client = Accumulo.newClient().from(props).build();
            HashMap<String, String> propMap = new HashMap<>();
            for (final String name : props.stringPropertyNames())
                  propMap.put(name, props.getProperty(name));

            propMap.put("table", "sampleTable");

            SparkConf conf = new SparkConf().setMaster("local").setAppName("AccumuloIntegrationTest");

            SparkSession sc = SparkSession.builder().config(conf).getOrCreate();

            Dataset<Row> sampleDf = sc.read()
                        // configure the header
                        .option("header", "true").option("inferSchema", "true")
                        // specify the file
                        .csv(Paths.get("target/test-classes/sample.txt").toUri().toString());

            sampleDf.show(10);
            sampleDf.printSchema();

            sampleDf.write().format("org.apache.accumulo").options(propMap).save();

            // read from accumulo
            StructType schema = new StructType(
                        new StructField[] { new StructField("label", DataTypes.DoubleType, true, Metadata.empty()),
                                    new StructField("text", DataTypes.StringType, true, Metadata.empty()),
                                    new StructField("count", DataTypes.IntegerType, true, Metadata.empty()) });
            Dataset<Row> accumuloDf = sc.read().format("org.apache.accumulo").options(propMap).schema(schema).load();

            accumuloDf.show(10);
      }
}