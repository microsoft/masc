package org.apache.accumulo.spark.decoder;

import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.data.Value;

public final class FloatStringEncodedValueDecoder implements ValueDecoder {
	@Override
	public Object decode(Value value) {
		// a) not happy about the allocation... can't we directly move the bytes into
		// Avro?
		// b) Charset should be configurable
		// TODO: check accumulo source if there is a reasonable efficient path without
		// all the allocation
		return Float.parseFloat(new String(value.get(), StandardCharsets.UTF_8));
	}
}
