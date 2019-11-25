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

package com.microsoft.accumulo.spark;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import com.microsoft.accumulo.spark.processors.AvroRowComputedColumns;
import com.microsoft.accumulo.spark.processors.AvroRowConsumer;
import com.microsoft.accumulo.spark.processors.AvroRowFilter;
import com.microsoft.accumulo.spark.processors.AvroRowMLeap;
import com.microsoft.accumulo.spark.processors.AvroRowSerializer;
import com.microsoft.accumulo.spark.record.AvroFastRecord;
import com.microsoft.accumulo.spark.record.AvroSchemaBuilder;
import com.microsoft.accumulo.spark.record.RowBuilderCellConsumer;
import com.microsoft.accumulo.spark.record.RowBuilderField;
import com.microsoft.accumulo.spark.util.StopWatch;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import java.nio.file.*;

/**
 * Backend iterator for Accumulo Connector for Apache Spark.
 * 
 * Features:
 * 
 * <ul>
 * <li>Combines selected key/value pairs into a single AVRO encoded row</li>
 * <li>Output schema convention: column family are top-level keys, column
 * qualifiers are nested record fields.</li>
 * <li>Row-level filtering through user-supplied Java Unified Expression
 * Language (JUEL)-encoded filter constraint.</li>
 * <li>Compute columns based on other row-level columns.</li>
 * <li>Schema less serialization performed to safe bandwidth.</li>
 * </ul>
 */
public class AvroRowEncoderIterator implements SortedKeyValueIterator<Key, Value>, OptionDescriber {
  private final static Logger logger = Logger.getLogger(AvroRowEncoderIterator.class);

  /**
   * Key for the schema input option.
   */
  public static final String SCHEMA = "schema";

  /**
   * Key for filter option.
   */
  public static final String FILTER = "filter";

  /**
   * Key for filter option.
   */
  public static final String MLEAP_FILTER = "mleapfilter";

  /**
   * Key for pruned columns.
   */
  public static final String PRUNED_COLUMNS = "prunedcolumns";

  /**
   * Key for path to exception log file. Can be handy if the logs are not
   * populated.
   */
  public static final String EXCEPTION_LOG_FILE = "exceptionlogfile";

  /**
   * A custom and fast implementation of an Avro record.
   */
  private AvroFastRecord rootRecord;

  /**
   * The final serializer creating the binary array.
   */
  private AvroRowSerializer serializer;

  /**
   * List of processors executed when the row was build up.
   */
  private List<AvroRowConsumer> processors;

  /**
   * Fast lookup table from "column family" to "column qualifier" to "type". If
   * it's not in this mapping we can skip the cell. Using this order as the cells
   * are sorted by family, qualifier
   */
  protected Map<ByteSequence, Map<ByteSequence, RowBuilderCellConsumer>> cellToColumnMap;

  /**
   * The source iterator;
   */
  protected SortedKeyValueIterator<Key, Value> sourceIter;

  /**
   * The current key.
   */
  private Key topKey = null;

  /**
   * The current value.
   */
  private Value topValue = null;

  private String exceptionLogFile;

  // private StopWatch stopWatchCellCollection;

  // private StopWatch stopWatchSerialization;

  private void logException(Throwable ex) {
    logger.error("Error", ex);

    if (this.exceptionLogFile != null) {
      try {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        Files.write(Paths.get(this.exceptionLogFile), (ex.getMessage().toString() + "\n" + sw.toString()).getBytes(),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      } catch (IOException e) {
        // swallow
      }
    }
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    try {
      // avoid Jackson to overcome version mismatch and compliance requirements
      new Gson().fromJson(options.get(SCHEMA), RowBuilderField[].class);

      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
      throws IOException {
    try {
      this.sourceIter = source;

      // keep the log file destination around
      this.exceptionLogFile = options.get(EXCEPTION_LOG_FILE);
      if (StringUtils.isEmpty(this.exceptionLogFile))
        this.exceptionLogFile = null;

      // this.stopWatchCellCollection = new StopWatch();
      // this.stopWatchSerialization = new StopWatch();

      // build the lookup table for the cells we care for from the user-supplied JSON
      // avoid Jackson to overcome version mismatch and compliance requirements
      RowBuilderField[] schemaFields = new Gson().fromJson(options.get(SCHEMA), RowBuilderField[].class);

      // union( user-supplied fields + computed fields )
      ArrayList<RowBuilderField> allFields = new ArrayList<>(Arrays.asList(schemaFields));

      logger.info("Initialize processors");

      this.processors = Arrays.stream(new AvroRowConsumer[] {
          // compute additional columns
          AvroRowComputedColumns.create(options),
          // filter row
          AvroRowFilter.create(options, FILTER),
          // apply ML model
          AvroRowMLeap.create(options),
          // filter post mleap
          AvroRowFilter.create(options, MLEAP_FILTER) })
          // compute & filter are optional depending on input
          .filter(Objects::nonNull).collect(Collectors.toList());

      // add all additional fields the consumers want to output
      allFields
          .addAll(this.processors.stream().flatMap(f -> f.getSchemaFields().stream()).collect(Collectors.toList()));

      // build the AVRO schema
      Schema schema = AvroSchemaBuilder.buildSchema(allFields);

      for (RowBuilderField rowBuilderField : allFields)
        logger.info("Output field: " + rowBuilderField.getColumnFamily() + ":" + rowBuilderField.getColumnQualifier()
            + ":" + rowBuilderField.getType());

      // initialize the record builder
      this.rootRecord = new AvroFastRecord(schema);

      // provide fast lookup map
      this.cellToColumnMap = AvroFastRecord.createCellToFieldMap(rootRecord);

      // feed the final schema back
      for (AvroRowConsumer consumer : this.processors)
        consumer.initialize(schema);

      // setup binary serializer
      this.serializer = new AvroRowSerializer(schema, options.get(PRUNED_COLUMNS));
    } catch (Throwable e) {
      logException(e);
      throw e;
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = new IteratorOptions("AvroRowEncodingIterator",
        "AvroRowEncodingIterator assists in building rows based on user-supplied schema.", null, null);

    io.addNamedOption(SCHEMA, "Schema selected cells of interest along with type information.");
    io.addNamedOption(FILTER, "JUEL encoded filter applied for each row.");
    io.addNamedOption(MLEAP_FILTER, "JUEL encoded filter applied after executing Mleap model.");
    io.addNamedOption(AvroRowMLeap.MLEAP_BUNDLE, "Base64 encoded Mleap bundle executed.");

    return io;
  }

  private void encodeRow() throws IOException {
    try {
      byte[] rowValue;
      Text currentRow;

      do {
        boolean foundConsumer = false;
        do {
          // no more input row?
          if (!sourceIter.hasTop()) {

            // // print performance statistics
            // logger.info(
            // String.format("Timing %22s: %.2fms", "cell collection",
            // this.stopWatchCellCollection.getAverage()));

            // logger
            // .info(String.format("Timing %22s: %.2fms", "serialization",
            // this.stopWatchSerialization.getAverage()));

            // for (AvroRowConsumer processor : this.processors)
            // logger.info(String.format("Timing %22s: %.2fms", processor.getName(),
            // processor.getAverageConsumeTime()));

            return;
          }

          // this.stopWatchCellCollection.start();

          currentRow = new Text(sourceIter.getTopKey().getRow());

          ByteSequence currentFamily = null;
          Map<ByteSequence, RowBuilderCellConsumer> currentQualifierMapping = null;

          // start of new record
          this.rootRecord.clear();

          while (sourceIter.hasTop() && sourceIter.getTopKey().getRow().equals(currentRow)) {
            Key sourceTopKey = sourceIter.getTopKey();

            // different column family?
            if (currentFamily == null || !sourceTopKey.getColumnFamilyData().equals(currentFamily)) {
              currentFamily = sourceTopKey.getColumnFamilyData();
              currentQualifierMapping = cellToColumnMap.get(currentFamily);
            }

            // skip if no mapping found
            if (currentQualifierMapping != null) {
              RowBuilderCellConsumer consumer = currentQualifierMapping.get(sourceTopKey.getColumnQualifierData());
              if (consumer != null) {
                foundConsumer = true;

                Value value = sourceIter.getTopValue();

                consumer.consume(sourceTopKey, value);
              }
            }

            // this.stopWatchCellCollection.stop();

            sourceIter.next();
          }
        } while (!foundConsumer); // skip rows until we found a single feature

        // produce final row
        rowValue = endRow(currentRow);
        // skip if null
      } while (rowValue == null);

      // null doesn't seem to be allowed for cf/cq...
      topKey = new Key(currentRow, new Text("v"), new Text(""));
      topValue = new Value(rowValue);

    } catch (Throwable e) {
      logException(e);
      throw e;
    }
  }

  private byte[] endRow(Text rowKey) throws IOException {
    // let's start the processing pipeline
    IndexedRecord record = this.rootRecord;

    for (AvroRowConsumer processor : this.processors) {
      if (!processor.consume(rowKey, record))
        // stop early
        return null;
    }

    // serialize the record
    // this.stopWatchSerialization.start();
    byte[] data = this.serializer.serialize(record);
    // this.stopWatchSerialization.stop();

    return data;
  }

  public Schema getSchema() {
    return this.rootRecord.getSchema();
  }

  @Override
  public Key getTopKey() {
    return topKey;
  }

  @Override
  public Value getTopValue() {
    return topValue;
  }

  @Override
  public boolean hasTop() {
    return topKey != null;
  }

  @Override
  public void next() throws IOException {
    topKey = null;
    topValue = null;
    encodeRow();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    topKey = null;
    topValue = null;

    // from RowEncodingIterator
    Key sk = range.getStartKey();

    if (sk != null && sk.getColumnFamilyData().length() == 0 && sk.getColumnQualifierData().length() == 0
        && sk.getColumnVisibilityData().length() == 0 && sk.getTimestamp() == Long.MAX_VALUE
        && !range.isStartKeyInclusive()) {
      // assuming that we are seeking using a key previously returned by this iterator
      // therefore go to the next row
      Key followingRowKey = sk.followingKey(PartialKey.ROW);
      if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0)
        return;

      range = new Range(sk.followingKey(PartialKey.ROW), true, range.getEndKey(), range.isEndKeyInclusive());
    }

    sourceIter.seek(range, columnFamilies, inclusive);
    encodeRow();
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    // add logging here
    AvroRowEncoderIterator copy = new AvroRowEncoderIterator();

    copy.serializer = new AvroRowSerializer(this.rootRecord.getSchema(), this.serializer.getPrunedColumns());
    copy.rootRecord = new AvroFastRecord(this.rootRecord.getSchema());
    copy.cellToColumnMap = AvroFastRecord.createCellToFieldMap(copy.rootRecord);
    copy.processors = this.processors.stream().map(AvroRowConsumer::clone).collect(Collectors.toList());
    copy.sourceIter = sourceIter.deepCopy(env);
    copy.exceptionLogFile = this.exceptionLogFile;

    return copy;
  }
}
