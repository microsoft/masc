package org.apache.accumulo.spark.el;

import java.util.Arrays;

import javax.el.ELContext;
import javax.el.ELException;
import javax.el.ValueExpression;

import org.apache.avro.generic.GenericData.Record;

public class AvroVariableExpression extends ValueExpression {

	private static final long serialVersionUID = 1L;

	private Class<?> type;
	private int[] fieldPositions;

	public AvroVariableExpression(Class<?> type, int... fieldPositions) {
		this.type = type;
		this.fieldPositions = fieldPositions;
	}

	@Override
	public Class<?> getExpectedType() {
		return type;
	}

	@Override
	public Class<?> getType(ELContext context) {
		return type;
	}

	@Override
	public Object getValue(ELContext context) {
		Record record = ((AvroContext) context).getAvroRecord();

		// supported nested records (e.g. column family/column qualifier)
		for (int i = 0; i < fieldPositions.length - 1; i++)
			record = (Record) record.get(fieldPositions[i]);

		return record.get(fieldPositions[fieldPositions.length - 1]);
	}

	@Override
	public boolean isReadOnly(ELContext context) {
		return true;
	}

	@Override
	public void setValue(ELContext context, Object value) {
		throw new ELException("setValue not supported");
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof AvroVariableExpression))
			return false;

		AvroVariableExpression other = (AvroVariableExpression) obj;

		return type.equals(other.type) && Arrays.equals(fieldPositions, other.fieldPositions);
	}

	@Override
	public String getExpressionString() {
		throw new ELException("getExpressionString() is not supported");
	}

	@Override
	public int hashCode() {
		return type.hashCode() + Arrays.hashCode(fieldPositions);
	}

	@Override
	public boolean isLiteralText() {
		return false;
	}
}