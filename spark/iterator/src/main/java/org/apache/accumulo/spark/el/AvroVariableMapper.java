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

		throw new IllegalArgumentException("Unable to find variable '" + variable + "' in schema");
	}

	@Override
	public ValueExpression resolveVariable(String variable) {
		if (variable.equals("rowKey"))
			return RowKeyVariableExpression.INSTANCE;

		SchemaMappingField field = findSchemaMappingFieldByVariable(variable);

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