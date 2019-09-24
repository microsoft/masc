package org.apache.accumulo.spark.el;

import javax.el.ELContext;
import javax.el.ELException;
import javax.el.ValueExpression;

import org.apache.hadoop.io.Text;

public class RowKeyVariableExpression extends ValueExpression {

	public static final RowKeyVariableExpression INSTANCE = new RowKeyVariableExpression();

	private static final long serialVersionUID = 1L;

	public RowKeyVariableExpression() {
	}

	@Override
	public Class<?> getExpectedType() {
		return String.class;
	}

	@Override
	public Class<?> getType(ELContext context) {
		return String.class;
	}

	@Override
	public Object getValue(ELContext context) {
		Text text = ((AvroContext) context).getRowKey();

		return text.toString();
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
		return obj instanceof RowKeyVariableExpression;
	}

	@Override
	public String getExpressionString() {
		throw new ELException("getExpressionString() is not supported");
	}

	@Override
	public int hashCode() {
		return 123;
	}

	@Override
	public boolean isLiteralText() {
		return false;
	}
}