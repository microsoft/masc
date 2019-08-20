package org.apache.accumulo.spark.decoder;

import org.apache.accumulo.core.data.Value;

public abstract class ValueDecoder {
  public String column;

  public abstract Object decode(Value value);
}
