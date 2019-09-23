package org.apache.accumulo.spark.el;

import javax.el.ELContext;
import javax.el.ELResolver;
import javax.el.FunctionMapper;
import javax.el.VariableMapper;

import org.apache.accumulo.spark.SchemaMappingField;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.io.Text;

public class AvroContext extends ELContext {

	private Record avroRecord;
	private Text rowKey;
	private VariableMapper variableMapper;
	private ELResolver resolver;

	public AvroContext(Schema schema, SchemaMappingField[] schemaMappingFields) {
		variableMapper = new AvroVariableMapper(schema, schemaMappingFields);
		resolver = new AvroResolver();
	}

	@Override
	public ELResolver getELResolver() {
		return resolver;
	}

	@Override
	public FunctionMapper getFunctionMapper() {
		return null;
	}

	@Override
	public VariableMapper getVariableMapper() {
		return variableMapper;
	}

	public Record getAvroRecord() {
		return avroRecord;
	}

	public Text getRowKey() {
		return rowKey;
	}

	public void setCurrent(Text rowKey, Record avroRecord) {
		this.rowKey = rowKey;
		this.avroRecord = avroRecord;
	}
}