
package org.apache.accumulo.spark;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.spark.decoder.StringValueDecoder;
import org.apache.accumulo.spark.decoder.ValueDecoder;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class BaseMappingIterator implements SortedKeyValueIterator<Key, Value>, OptionDescriber {
  private static final String SCHEMA = "schema";

  // Column Family -> Column Qualifier -> Namespace
  // Using this order as the cells are sorted by family, qualifier
  private HashMap<ByteSequence, HashMap<ByteSequence, ValueDecoder>> cellToColumnMap;

  protected SortedKeyValueIterator<Key, Value> sourceIter;
  protected SchemaMappingField[] schemaMappingFields;

  private Key topKey = null;
  private Value topValue = null;

  protected abstract void startRow(Text rowKey) throws IOException;

  protected abstract void processCell(Key key, Value value, Object decodedValue) throws IOException;

  protected abstract byte[] endRow() throws IOException;

  protected ValueDecoder getDecoder(String type) {
    if (type.equalsIgnoreCase("string"))
      return new StringValueDecoder();

    // TODO: additional types
    // string the encoding topic (utf8)
    // string encoded numbers (e.g. "123")

    throw new IllegalArgumentException("Unsupported type: '" + type + "'");
  }

  private void encodeRow() throws IOException {
    Text currentRow;
    boolean foundFeature = false;
    do {
      if (!sourceIter.hasTop())
        return;
      currentRow = new Text(sourceIter.getTopKey().getRow());

      ByteSequence currentFamily = null;
      Map<ByteSequence, ValueDecoder> currentQualifierMapping = null;

      // dispatch
      startRow(currentRow);

      while (sourceIter.hasTop() && sourceIter.getTopKey().getRow().equals(currentRow)) {
        Key sourceTopKey = sourceIter.getTopKey();

        // different column family?
        if (currentFamily == null || !sourceTopKey.getColumnFamilyData().equals(currentFamily)) {
          currentFamily = sourceTopKey.getColumnFamilyData();
          currentQualifierMapping = cellToColumnMap.get(currentFamily);
        }

        // skip if no mapping found
        if (currentQualifierMapping == null)
          continue;

        ValueDecoder featurizer = currentQualifierMapping.get(sourceTopKey.getColumnQualifierData());
        if (featurizer == null)
          continue;

        foundFeature = true;

        Value value = sourceIter.getTopValue();

        processCell(sourceTopKey, value, featurizer.decode(value));

        sourceIter.next();
      }
    } while (!foundFeature); // skip rows until we found a single feature

    // null doesn't seem to be allowed for cf/cq...
    topKey = new Key(currentRow, new Text("a"), new Text("b"));
    topValue = new Value(endRow());
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
    BaseMappingIterator copy;
    try {
      copy = this.getClass().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // TODO: right now it's immutable, thus shallow = deep
    copy.cellToColumnMap = cellToColumnMap;
    copy.sourceIter = sourceIter.deepCopy(env);

    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = new IteratorOptions("spark.dataframe", "Spark DataFrame", null, null);

    io.addNamedOption("schema", "Schema mapping cells into columns");

    return io;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    // parseFeaturizer(options);
    // TODO: parse JSON

    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
      throws IOException {
    sourceIter = source;

    ObjectMapper objectMapper = new ObjectMapper();
    schemaMappingFields = objectMapper.readValue(options.get(SCHEMA), SchemaMappingField[].class);

    cellToColumnMap = new HashMap<>();
    for (SchemaMappingField schemaMappingField : schemaMappingFields) {
      ByteSequence columnFamily = new ArrayByteSequence(schemaMappingField.getColumnFamily());
      HashMap<ByteSequence, ValueDecoder> qualifierMap = cellToColumnMap.get(columnFamily);

      if (qualifierMap == null) {
        qualifierMap = new HashMap<>();
        cellToColumnMap.put(columnFamily, qualifierMap);
      }

      ByteSequence columnQualifier = new ArrayByteSequence(schemaMappingField.getColumnQualifier());

      // find the decoder for the respective type
      ValueDecoder valueDecoder = getDecoder(schemaMappingField.getType());

      qualifierMap.put(columnQualifier, valueDecoder);
    }
  }
}
