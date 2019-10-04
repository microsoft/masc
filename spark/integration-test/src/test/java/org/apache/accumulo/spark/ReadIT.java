package org.apache.accumulo.spark;

import java.io.File;
import java.util.Properties;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import java.util.Map.Entry;
import org.apache.spark.ml.feature.RegexTokenizer;

public class ReadIT {

      @Test
      public void testDataReading() throws Exception {
            File propsFile = new File("target/accumulo2-maven-plugin/spark-connector-instance");
            Properties props = MiniAccumuloCluster.getClientProperties(propsFile);
            // AccumuloClient client = Accumulo.newClient().from(props).build();

            // String inputTable = "manual_table";
            // client.tableOperations().create(inputTable);
            // IntegerLexicoder lexicoder = new IntegerLexicoder();
            // // Write data to input table
            // try (BatchWriter bw = client.createBatchWriter(inputTable)) {
            // for (int i = 0; i < 5; i++) {
            // Mutation m = new Mutation(String.format("%03d", i));
            // m.at().family("cf1").qualifier("cq1").put(lexicoder.encode(i));
            // if (i % 2 == 0)
            // m.at().family("cf3").qualifier("").put("c" + i);

            // bw.addMutation(m);
            // }
            // }

            // try (Scanner scanner = client.createScanner(inputTable,
            // Authorizations.EMPTY)) {
            // for (Entry<Key, Value> entry : scanner) {
            // System.out.println(entry.getKey().toString() + " -> " +
            // entry.getValue().toString());
            // }
            // }

            HashMap<String, String> propMap = new HashMap<>();
            for (final String name : props.stringPropertyNames())
                  propMap.put(name, props.getProperty(name));

            propMap.put("table", "sample_table");

            SparkConf conf = new SparkConf().setMaster("local").setAppName("AccumuloIntegrationTest");

            SparkSession sc = SparkSession.builder().config(conf).getOrCreate();

            Dataset<Row> sampleDf = sc.read()
                        // configure the header
                        .option("header", "true").option("inferSchema", "true")
                        // specify the file
                        .csv(Paths.get("target/test-classes/sample.txt").toUri().toString());

            sampleDf.show(10);
            sampleDf.printSchema();


            RegexTokenizer tokenizer = new RegexTokenizer()
                  .setGaps(false)
                  .setPattern("\\p{L}+")
                  .setInput
            
            gaps=False, pattern='\\p{L}+', inputCol='text', outputCol='words')
vectorizer = CountVectorizer(inputCol='words', outputCol='features')
lr = LogisticRegression(maxIter=1, regParam=0.2, elasticNetParam=0)
pipeline = Pipeline(stages=[tokenizer, vectorizer, lr])



            propMap.put("rowkey", "key");
            sampleDf.write().format("org.apache.accumulo").options(propMap).save();

            // try (Scanner scanner = client.createScanner("sample_table",
            // Authorizations.EMPTY)) {
            // for (Entry<Key, Value> entry : scanner) {
            // System.out.println("CQ: " + entry.getKey().getColumnQualifier());
            // System.out.println("CQ.length: " +
            // entry.getKey().getColumnQualifierData().length());
            // System.out.println(entry.getKey().toString() + " -> " +
            // entry.getValue().toString());
            // }
            // }

            propMap.put("mleap.bundle", "asdfasdfasdf");

            // read from accumulo
            StructType schema = new StructType(
                        new StructField[] { new StructField("label", DataTypes.DoubleType, true, Metadata.empty()),
                                    new StructField("text", DataTypes.StringType, true, Metadata.empty()),
                                    new StructField("count", DataTypes.IntegerType, true, Metadata.empty()) });
            Dataset<Row> accumuloDf = sc.read().format("org.apache.accumulo").options(propMap).schema(schema).load();

            accumuloDf.show(10);

      }
}