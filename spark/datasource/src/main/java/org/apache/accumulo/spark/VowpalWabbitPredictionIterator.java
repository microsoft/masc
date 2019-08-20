package org.apache.accumulo.spark;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class VowpalWabbitPredictionIterator
    implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
  // TODO: VW Model
  private static final String MODEL = "model";
  // private static final String FEATURE_COLUMN_FAMILY = "columnFamily";
  // private static final String FEATURE_COLUMN_QUALIFIER = "columnQualifier";
  // TODO: is this safe (as in immutable)?
  private static final Text OUTPUT_COLUMN_FAMILY = new Text("vw");
  private static final Text OUTPUT_COLUMN_QUALIFIER_SCALAR = new Text("prediction");
  private static final Pattern COLUMN_FAMILY_AND_QUALIFIER_REGEX = Pattern
      .compile("^_([a-fA-F0-9]+)_");

  private static class StringCellFeaturizer extends CellFeaturizer {

    // TODO: add encoding
    public StringCellFeaturizer(String name, String type) {
      super(name, type);
    }

    @Override
    void featurize(int example, Value value) {
      try {
        // TODO: add encoding support
        // TODO: featurize here
        new String(value.get(), 0, value.getSize(), StandardCharsets.UTF_8.name());
      } catch (UnsupportedEncodingException ex) {
        throw new IllegalArgumentException("Unsupported encoding 'TODO'");
      }
    }
  }

  private abstract static class CellFeaturizer {
    protected final int hash;
    protected final String name;
    protected final char featureGroup;

    /**
     * @param name
     *          Vowpal Wabbit namespace.
     * @param type
     *          How to interpret the value
     */
    public CellFeaturizer(String name, String type) {
      this.name = name;
      this.featureGroup = name.charAt(0);
      this.hash = 0; // TODO
    }

    abstract void featurize(int example, Value value);
  }

  // Column Family -> Column Qualifier -> Namespace
  // Using this order as the cells are sorted by family, qualifier
  private HashMap<ByteSequence,HashMap<ByteSequence,CellFeaturizer>> cellToFeatureMap;
  // private Text featureColumnFamily;
  // private Text featureColumnQualifier;
  private byte[] model;

  protected SortedKeyValueIterator<Key,Value> sourceIter;
  private Key topKey = null;
  private Value topValue = null;

  // TODO: prep for withColumn() semantic
  // private List<Key> keys = new ArrayList<>();
  // private List<Value> values = new ArrayList<>();

  private void predict() throws IOException {
    Text currentRow;
    boolean foundFeature = false;
    do {
      if (!sourceIter.hasTop())
        return;
      currentRow = new Text(sourceIter.getTopKey().getRow());
      // keys.clear();
      // values.clear();
      ByteSequence currentFamily = null;
      Map<ByteSequence,CellFeaturizer> currentQualifierMapping = null;

      while (sourceIter.hasTop() && sourceIter.getTopKey().getRow().equals(currentRow)) {
        Key sourceTopKey = sourceIter.getTopKey();

        // different column family?
        if (currentFamily == null || !sourceTopKey.getColumnFamilyData().equals(currentFamily)) {
          currentFamily = sourceTopKey.getColumnFamilyData();
          currentQualifierMapping = cellToFeatureMap.get(currentFamily);
        }

        // skip if no mapping found
        if (currentQualifierMapping == null)
          continue;

        CellFeaturizer featurizer = currentQualifierMapping
            .get(sourceTopKey.getColumnQualifierData());
        if (featurizer == null)
          continue;

        foundFeature = true;

        // build VW example
        // TODO: add to VW example
        featurizer.featurize(0, sourceIter.getTopValue());

        // TODO: if we want to support Spark withColumn-style operation we need to
        // collect...
        // on match
        // foundFeature = true;
        // keys.add(new Key(sourceTopKey));
        // values.add(new Value(sourceTopValue));

        sourceIter.next();
      }
    } while (!foundFeature); // skip rows until we found a single feature

    topKey = new Key(currentRow, OUTPUT_COLUMN_FAMILY, OUTPUT_COLUMN_QUALIFIER_SCALAR);
    topValue = new Value(new Text("42"));
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
    predict();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    topKey = null;
    topValue = null;

    // from RowEncodingIterator
    Key sk = range.getStartKey();

    if (sk != null && sk.getColumnFamilyData().length() == 0
        && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
        && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
      // assuming that we are seeking using a key previously returned by this iterator
      // therefore go to the next row
      Key followingRowKey = sk.followingKey(PartialKey.ROW);
      if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0)
        return;

      range = new Range(sk.followingKey(PartialKey.ROW), true, range.getEndKey(),
          range.isEndKeyInclusive());
    }

    sourceIter.seek(range, columnFamilies, inclusive);
    predict();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    VowpalWabbitPredictionIterator copy = new VowpalWabbitPredictionIterator();

    copy.model = Arrays.copyOf(model, model.length);
    // TODO: right now it's immutable...
    copy.cellToFeatureMap = cellToFeatureMap;
    copy.sourceIter = sourceIter.deepCopy(env);

    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = new IteratorOptions("vw", "VowpalWabbit performs prediction", null, null);

    io.addNamedOption(MODEL, "Vowpal Wabbit binary model");

    // TODO: how is this surfaced?

    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    String modelBase64Encoded = options.get(MODEL);
    if (modelBase64Encoded == null)
      return false;

    parseModel(options);
    parseFeaturizer(options);

    return true;
  }

  // TODO: move to factory class
  private static CellFeaturizer createFeaturizer(String namespace, String type) {
    switch (type.charAt(0)) {
      case 's':
        return new StringCellFeaturizer(namespace, type);
      default:
        throw new IllegalArgumentException(
            "Unsupported type '" + type + "' for namespace '" + namespace + "'");
    }
  }

  private static byte[] parseModel(Map<String,String> options) {
    try {
      return Base64.getDecoder().decode(options.get(MODEL));
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable decode '" + MODEL + "' option", e);
    }
  }

  private static HashMap<ByteSequence,HashMap<ByteSequence,CellFeaturizer>> parseFeaturizer(
      Map<String,String> options) {
    HashMap<ByteSequence,HashMap<ByteSequence,CellFeaturizer>> cellToFeatureMap = new HashMap<>();
    for (Map.Entry<String,String> entry : options.entrySet()) {
      if (!entry.getKey().startsWith("_"))
        continue;

      // parse column spec and create sub-map if not there
      ByteSequence[] familyAndQualifier = decodeColumn(entry.getKey());
      HashMap<ByteSequence,CellFeaturizer> qualifierMap = cellToFeatureMap
          .get(familyAndQualifier[0]);
      if (qualifierMap == null) {
        qualifierMap = new HashMap<>();
        cellToFeatureMap.put(familyAndQualifier[0], qualifierMap);
      }

      // decode target spec: type|namespace
      String[] typeAndNamespace = entry.getValue().split("|");
      String namespace = typeAndNamespace[1];
      String type = typeAndNamespace[0];

      qualifierMap.put(familyAndQualifier[1], createFeaturizer(namespace, type));
    }

    return cellToFeatureMap;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    sourceIter = source;

    model = parseModel(options);
    cellToFeatureMap = parseFeaturizer(options);
  }

  public static void setModel(IteratorSetting cfg, byte[] model) {
    cfg.addOption(MODEL, Base64.getEncoder().encodeToString(model));
  }

  private static String encodeColumn(String columnFamily, String columnQualifier) {
    // Format: length-of-columnFamily-in_hex_columnFamily_columnQualifier
    // TODO: extend this language to support columnFamily, columnQualifier or value
    // as feature input (need to encode "wildcard", e.g. null?)
    return String.format("_%x_%s_%s", columnFamily.length(), columnFamily, columnQualifier);
  }

  private static ByteSequence[] decodeColumn(String columnFamilyAndQualifier) {
    Matcher m = COLUMN_FAMILY_AND_QUALIFIER_REGEX.matcher(columnFamilyAndQualifier);

    if (!m.find())
      throw new IllegalArgumentException(
          "Unsupported setting. Must follow _<column-family-length-in-hex>_<column-family>_<column-qualifier>");

    // parse hex encoded family length
    int columnFamilyLength = Integer.parseInt(m.group(1), 16);
    int columnFamilyStart = m.end();

    return new ByteSequence[] {
        new ArrayByteSequence(columnFamilyAndQualifier.substring(columnFamilyStart,
            columnFamilyStart + columnFamilyLength)),
        new ArrayByteSequence(columnFamilyAndQualifier
            .substring(columnFamilyStart + columnFamilyLength + 1 /* skip _ */))};
  }

  public static void addFeatureMapping(IteratorSetting cfg, String columnFamily,
      String columnQualifier, String namespace, String type) {

    // | is not a valid namespace character
    cfg.addOption(encodeColumn(columnFamily, columnQualifier), type + "|" + namespace);
  }
}
