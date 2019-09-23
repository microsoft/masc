package org.apache.accumulo.spark.el;

import javax.el.ELContext;
import javax.el.ELException;
import javax.el.ValueExpression;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.Schema.Field;

public class AvroObjectExpression extends ValueExpression {

	private static final long serialVersionUID = 1L;

	private Field field;

	public AvroObjectExpression(Field field) {
		this.field = field;
	}

	@Override
	public Class<?> getExpectedType() {
		return Record.class;
	}

	@Override
	public Class<?> getType(ELContext context) {
		return Record.class;
	}

	@Override
	public Object getValue(ELContext context) {
		Record record = ((AvroContext) context).getAvroRecord();

		return (Record) record.get(this.field.pos());
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

		AvroObjectExpression other = (AvroObjectExpression) obj;

		return this.field.equals(other.field);
	}

	@Override
	public String getExpressionString() {
		throw new ELException("getExpressionString() is not supported");
	}

	@Override
	public int hashCode() {
		return this.field.hashCode();
	}

	@Override
	public boolean isLiteralText() {
		return false;
	}
}