package org.apache.accumulo.spark.el;

import javax.el.ValueExpression;
import javax.el.VariableMapper;

import org.apache.accumulo.spark.SchemaMappingField;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

public class AvroVariableMapper extends VariableMapper {

	private Schema schema;
	private SchemaMappingField[] schemaMappingFields;

	public AvroVariableMapper(Schema schema, SchemaMappingField[] schemaMappingFields) {
		this.schema = schema;
		this.schemaMappingFields = schemaMappingFields;
	}

	private SchemaMappingField findSchemaMappingFieldByVariable(String variable) {
		for (SchemaMappingField smf : schemaMappingFields)
			if (smf.getFilterVariableName().equals(variable))
				return smf;

		return null;
	}

	@Override
	public ValueExpression resolveVariable(String variable) {
		if (variable.equals("rowKey"))
			return RowKeyVariableExpression.INSTANCE;

		// check if this is a statically resolved variable (e.g. v2 = cf1.cq1)
		SchemaMappingField field = findSchemaMappingFieldByVariable(variable);
		if (field == null)
			return new AvroObjectExpression(schema.getField(variable));

		Field columnFamilyField = schema.getField(field.getColumnFamily());

		if (field.getColumnQualifier() == null || field.getColumnQualifier().isEmpty())
			return new AvroVariableExpression(field.getJavaType(), columnFamilyField.pos());

		Field columnQualifierField = columnFamilyField.schema().getField(field.getColumnQualifier());
		return new AvroVariableExpression(field.getJavaType(), columnFamilyField.pos(), columnQualifierField.pos());
	}

	@Override
	public ValueExpression setVariable(String variable, ValueExpression expression) {
		return null;
	}

}