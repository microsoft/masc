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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.spark.processors.AvroRowMLeap;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
// import org.apache.spark.SparkConf;
// import org.apache.spark.sql.SparkSession;
import org.junit.Test;

// import org.apache.spark.ml.feature.RegexTokenizer;
// import org.apache.spark.ml.feature.CountVectorizer;
// import org.apache.spark.ml.classification.DecisionTreeClassifier;
// import org.apache.spark.ml.classification.LogisticRegression;
// import org.apache.spark.ml.classification.LogisticRegressionModel;
// import org.apache.spark.ml.Pipeline;
// import org.apache.spark.ml.PipelineStage;
// import org.apache.spark.ml.PipelineModel;
// import org.apache.spark.ml.mleap.SparkUtil;
// import org.apache.spark.ml.regression.DecisionTreeRegressor;

import ml.combust.bundle.BundleFile;
import ml.combust.bundle.serializer.SerializationFormat;
import ml.combust.mleap.runtime.transformer.regression.DecisionTreeRegression;
// import ml.combust.mleap.spark.SimpleSparkSerializer;
// import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.Row;
// import org.apache.spark.sql.SparkSession;
// import org.apache.spark.sql.types.DataTypes;
// import org.apache.spark.sql.types.Metadata;
// import org.apache.spark.sql.types.StructField;
// import org.apache.spark.sql.types.StructType;
import java.io.File;

import com.google.common.io.Resources;

public class AvroMLeapSentimentTest {

	// Tried to
	// private String trainMLeapModel(Pipeline pipeline) throws Exception {
	// SparkConf conf = new
	// SparkConf().setMaster("local").setAppName("AccumuloIteratorTest");

	// SparkSession sc = SparkSession.builder().config(conf).getOrCreate();

	// Dataset<Row> sampleDf = sc.read()
	// // configure the header
	// .option("header", "true").option("inferSchema", "true")
	// // specify the file
	// .csv(AvroMLeapTest.class.getResource("sample.txt").toURI().toString());

	// // sampleDf.show(10);
	// // sampleDf.printSchema();

	// // train the model
	// PipelineModel model = pipeline.fit(sampleDf);

	// File tempModel = File.createTempFile("mleap", ".zip");
	// tempModel.delete();
	// tempModel.deleteOnExit(); // just in case something goes wrong

	// new SimpleSparkSerializer().serializeToBundle(model, "jar:" +
	// tempModel.toPath().toUri().toString(),
	// model.transform(sampleDf));

	// byte[] modelByteArray = Files.readAllBytes(tempModel.toPath());

	// tempModel.delete();

	// return Base64.getEncoder().encodeToString(modelByteArray);
	// }

	// private String trainSentimentModel() throws Exception {
	// // build SparkML pipeline
	// RegexTokenizer tokenizer = new
	// RegexTokenizer().setGaps(false).setPattern("\\p{L}+").setInputCol("text")
	// .setOutputCol("words");
	// CountVectorizer vectorizer = new
	// CountVectorizer().setInputCol("words").setOutputCol("features");
	// DecisionTreeRegressor model = new DecisionTreeRegressor();

	// Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { tokenizer,
	// vectorizer, model });

	// return trainMLeapModel(pipeline);
	// }

	private AvroRowEncoderIterator createIterator(String mleapFilter) throws Exception {
		// load mleap model
		byte[] mleapBundle = Resources.toByteArray(AvroMLeapSentimentTest.class.getResource("sentiment.zip"));
		// byte[] mleapBundle =
		// Resources.toByteArray(AvroMLeapSentimentTest.class.getResource("twitter.model.lr.zip"));
		String mleapBundleBase64 = Base64.getEncoder().encodeToString(mleapBundle);

		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "text", ""), new Value(new StringLexicoder().encode("this is good")));
		map.put(new Key("key2", "text", ""), new Value(new StringLexicoder().encode("this is bad")));
		// for (int i = 3; i < 8 * 1024; i++) {
		// map.put(new Key("keyX" + i, "text", ""), new Value(new
		// StringLexicoder().encode(
		// "this is bad very very long text " + i + " with a lot of data" + i + " and
		// some more characters")));
		// }

		SortedMapIterator parentIterator = new SortedMapIterator(map);
		AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

		Map<String, String> options = new HashMap<>();
		options.put(AvroRowEncoderIterator.SCHEMA, "[{\"cf\":\"text\",\"t\":\"string\"}]");

		// pass the model to the iterator
		options.put(AvroRowMLeap.MLEAP_BUNDLE, mleapBundleBase64);

		if (StringUtils.isNotBlank(mleapFilter))
			options.put(AvroRowEncoderIterator.MLEAP_FILTER, mleapFilter);

		iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
		iterator.seek(new Range(), AvroUtil.EMPTY_SET, false);

		return iterator;
	}

	@Test
	public void testMLeapModelExecution() throws Exception {
		BasicConfigurator.configure();

		AvroRowEncoderIterator iterator = createIterator(null);

		// row 1
		assertTrue(iterator.hasTop());
		GenericRecord record = AvroUtil.deserialize(iterator.getTopValue().get(), iterator.getSchema());

		assertEquals("key1", iterator.getTopKey().getRow().toString());
		assertEquals(1.0, (double) record.get("prediction"), 0.00001); // disable for perf test

		// row2
		iterator.next();

		assertTrue(iterator.hasTop());
		record = AvroUtil.deserialize(iterator.getTopValue().get(), iterator.getSchema());

		assertEquals("key2", iterator.getTopKey().getRow().toString());
		System.out.println(record.get("prediction"));
		assertEquals(0.0, (double) record.get("prediction"), 0.00001);

		// perf test
		// iterator.next();

		// for (; iterator.hasTop(); iterator.next()) {
		// // assertEquals(0.0, (double) record.get("prediction"), 0.00001);
		// assertEquals(0.0, (double) record.get("prediction"), 0.00001);
		// double x = (double) record.get("prediction");
		// }

		// end
		iterator.next();
		assertFalse(iterator.hasTop());
	}
}
