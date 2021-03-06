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

package com.microsoft.accumulo.spark.processors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.microsoft.accumulo.spark.record.RowBuilderField;
import com.microsoft.accumulo.spark.record.RowBuilderType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import com.microsoft.accumulo.zipfs.ZipFileSystem;
import com.microsoft.accumulo.zipfs.ZipFileSystemProvider;

import ml.combust.mleap.avro.SchemaConverter;
import ml.combust.mleap.core.types.BasicType;
import ml.combust.mleap.core.types.DataType;
import ml.combust.mleap.core.types.ScalarType;
import ml.combust.mleap.core.types.StructField;
import ml.combust.mleap.core.types.StructType;
import ml.combust.mleap.runtime.MleapContext;
import ml.combust.mleap.runtime.frame.ArrayRow;
import ml.combust.mleap.runtime.frame.DefaultLeapFrame;
import ml.combust.mleap.runtime.frame.Row;
import ml.combust.mleap.runtime.frame.Transformer;
import ml.combust.mleap.runtime.frame.RowTransformer;
import ml.combust.bundle.BundleFile;
import ml.combust.mleap.runtime.javadsl.ContextBuilder;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
// https://github.com/marschall/memoryfilesystem has a 16MB file size limitation
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import java.nio.file.Path;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

/**
 * Maps AVRO Generic row to MLeap data frame enabling server-side inference.
 */
public class AvroRowMLeap extends AvroRowConsumer {
  private final static Logger logger = Logger.getLogger(AvroRowMLeap.class);

  private final static Cache<String, Transformer> transformers = CacheBuilder.newBuilder()
      // expectation is that iterators are re-inited rather frequently
      .expireAfterAccess(5, TimeUnit.MINUTES)
      // build up
      .build();

  private static Transformer CreateTransformer(String mleapGuid, String mleapBundleBase64) throws IOException {
    long start = System.nanoTime();

    byte[] mleapBundle = Base64.getDecoder().decode(mleapBundleBase64);

    FileSystem fs = Jimfs.newFileSystem(Configuration.unix());
    Path mleapFilePath = fs.getPath("/mleap.zip");
    Files.write(mleapFilePath, mleapBundle, StandardOpenOption.CREATE);

    MleapContext mleapContext = new ContextBuilder().createMleapContext();

    try (
        FileSystem zfs = new ZipFileSystem(new ZipFileSystemProvider(), mleapFilePath, new HashMap<>())) {
      try (BundleFile bf = new BundleFile(zfs, zfs.getPath("/"))) {
        Transformer transformer = (Transformer) bf.load(mleapContext).get().root();

        logger.info(String.format("Decompressing model (%.1fkb) %.2fms cache id: %s",
            mleapBundleBase64.length() / 1024.0, (System.nanoTime() - start) / 1e6, mleapGuid));

        return transformer;
      }
    }
  }

  /**
   * Key for mleap bundle option.
   */
  public static final String MLEAP_BUNDLE = "mleap";

  /**
   * GUID to support fast caching.
   */
  public static final String MLEAP_GUID = "mleapguid";

  public static AvroRowMLeap create(Map<String, String> options) throws IOException {
    String mleapBundleBase64 = options.get(MLEAP_BUNDLE);
    String mleapGuid = options.get(MLEAP_GUID);

    if (StringUtils.isEmpty(mleapBundleBase64) || StringUtils.isEmpty(mleapGuid))
      return null;

    try {
      return new AvroRowMLeap(transformers.get(mleapGuid, () -> CreateTransformer(mleapGuid, mleapBundleBase64)));
    } catch (ExecutionException e) {
      throw (IOException) e.getCause();
    }
  }

  /**
   * Definition of the output fields.
   */
  private static class OutputField {
    private RowBuilderField field;

    private int outputFieldIndex;

    private int avroFieldIndex;

    public OutputField(RowBuilderField field) {
      this.field = field;
    }

    public RowBuilderField getField() {
      return field;
    }

    public int getOutputFieldIndex() {
      return outputFieldIndex;
    }

    public void setOutputFieldIndex(int outputFieldIndex) {
      this.outputFieldIndex = outputFieldIndex;
    }

    public void setAvroFieldIndex(int avroFieldIndex) {
      this.avroFieldIndex = avroFieldIndex;
    }

    public int getAvroFieldIndex() {
      return avroFieldIndex;
    }

    @Override
    public String toString() {
      return String.format("%s: %d <- %d", field.getColumnFamily(), outputFieldIndex, avroFieldIndex);
    }
  }

  private List<OutputField> outputFields;
  private Schema schema;
  private Object[] mleapValues;
  private Transformer transformer;
  private RowTransformer rowTransformer;
  private ArrayRow arrayRow;
  private int[] inputIndices;
  private int[] outputIndicesSource;
  private int[] outputIndicesDest;

  private AvroRowMLeap(Transformer transformer) {
    this.transformer = transformer;

    // logger.info("Transformer: " + this.transformer.model());
    // logger.info("Input schema: " + this.transformer.inputSchema());
    // logger.info("Output schema: " + this.transformer.outputSchema());

    // convert the output schema and remember the field indices
    StructType outputSchema = this.transformer.outputSchema();

    this.outputFields = JavaConverters.seqAsJavaListConverter(outputSchema.fields()).asJava().stream()
        // main loop
        .map(field -> {
          DataType dt = field.dataType();
          String name = field.name();

          if (!(dt instanceof ScalarType))
            return null;

          ScalarType scalarType = (ScalarType) dt;
          if (BasicType.Boolean$.MODULE$.equals(scalarType.base()))
            return new OutputField(new RowBuilderField(name, null, RowBuilderType.Boolean.toString(), name));

          if (BasicType.Double$.MODULE$.equals(scalarType.base()))
            return new OutputField(new RowBuilderField(name, null, RowBuilderType.Double.toString(), name));

          if (BasicType.Float$.MODULE$.equals(scalarType.base()))
            return new OutputField(new RowBuilderField(name, null, RowBuilderType.Float.toString(), name));

          if (BasicType.Int$.MODULE$.equals(scalarType.base()))
            return new OutputField(new RowBuilderField(name, null, RowBuilderType.Integer.toString(), name));

          if (BasicType.Short$.MODULE$.equals(scalarType.base()))
            return new OutputField(new RowBuilderField(name, null, RowBuilderType.Integer.toString(), name));

          if (BasicType.Long$.MODULE$.equals(scalarType.base()))
            return new OutputField(new RowBuilderField(name, null, RowBuilderType.Long.toString(), name));

          return null;
        })
        // drop non-supported
        .filter(Objects::nonNull)
        // configure non-nullable to be compatible with MLeap to Spark type conversation
        .peek(field -> field.getField().setNullable(false)).collect(Collectors.toList());
  }

  @Override
  public AvroRowMLeap clone() {
    AvroRowMLeap copy = new AvroRowMLeap(this.transformer);

    copy.initialize(schema);

    return copy;
  }

  @Override
  public Collection<RowBuilderField> getSchemaFields() {
    return this.outputFields.stream().map(OutputField::getField).collect(Collectors.toList());
  }

  @Override
  public void initialize(Schema schema) {
    this.schema = schema;

    // mapping the Avro schema to MLeap schema
    List<Field> avroFields = new ArrayList<>();

    List<StructField> mleapFields = new ArrayList<>();
    for (Field field : schema.getFields()) {
      if (field.schema().getType() == Type.RECORD)
        continue;

      // make sure we don't have duplicate fields
      if (this.outputFields.stream().anyMatch(f -> f.getField().getColumnFamily().equals(field.name())))
        continue;

      avroFields.add(field);
      mleapFields.add(SchemaConverter.avroToMleapField(field, null));
      // logger.info("Input field: " + field);
    }

    Field[] mleapAvroFields = avroFields.toArray(new Field[0]);
    StructType mleapSchema = StructType.apply(mleapFields).get();
    this.mleapValues = new Object[mleapAvroFields.length];

    this.rowTransformer = transformer.transform(RowTransformer.apply(mleapSchema)).get();
    this.arrayRow = new ArrayRow(WrappedArray.make(this.mleapValues));

    // generate the final schema
    StructType outputSchema = this.transformer
        // could also use scala.collection.Seq$.MODULE$.empty() but we'd get a type
        // warning
        .transform(new DefaultLeapFrame(mleapSchema, scala.collection.Seq$.MODULE$.<Row>newBuilder().result()))
        .get().schema();

    for (OutputField field : this.outputFields) {
      // correct output index by the number of fields we input
      field.setOutputFieldIndex((int) outputSchema.indexOf(field.field.getColumnFamily()).get());

      // link mleap dataframe field index with avro field index
      field.setAvroFieldIndex(schema.getField(field.getField().getColumnFamily()).pos());

      logger.info("Output field: " + field.getField().getColumnFamily() + ":" + field.getField().getType());
    }

    // cache indices
    this.inputIndices = new int[mleapAvroFields.length];

    for (int i = 0; i < mleapAvroFields.length; i++)
      this.inputIndices[i] = mleapAvroFields[i].pos();

    this.outputIndicesSource = new int[this.outputFields.size()];
    this.outputIndicesDest = new int[this.outputFields.size()];

    for (int i = 0; i < this.outputFields.size(); i++) {
      OutputField of = this.outputFields.get(i);
      this.outputIndicesSource[i] = of.getOutputFieldIndex();
      this.outputIndicesDest[i] = of.getAvroFieldIndex();
    }
  }

  @Override
  protected boolean consumeInternal(Text rowKey, IndexedRecord record) {
    // surface data to MLeap dataframe
    // Note: experimented with a wrapper that around the AvroRecord and it doesn't change anything in performance
    for (int i = 0; i < this.inputIndices.length; i++)
      this.mleapValues[i] = record.get(this.inputIndices[i]);

    Row row = this.rowTransformer.transform(this.arrayRow);

    // copy mleap output to avro record
    for (int i = 0; i < this.outputIndicesSource.length; i++) 
      record.put(this.outputIndicesDest[i], row.get(this.outputIndicesSource[i]));

    return true;
  }
}
