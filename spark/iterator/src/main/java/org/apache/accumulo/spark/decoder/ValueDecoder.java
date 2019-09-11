package org.apache.accumulo.spark.decoder;

import org.apache.accumulo.core.data.Value;

public interface ValueDecoder {
  public Object decode(Value value);
}
