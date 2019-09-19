package org.apache.accumulo.spark;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class AvroUtil {
	private static SchemaBuilder.FieldAssembler<Schema> addAvroField(SchemaBuilder.FieldAssembler<Schema> builder,
			String type, String name) {
		switch (type.toUpperCase()) {
		case "STRING":
			return builder.optionalString(name);

		case "LONG":
			return builder.optionalLong(name);

		case "INTEGER":
			return builder.optionalInt(name);

		case "DOUBLE":
			return builder.optionalDouble(name);

		case "FLOAT":
			return builder.optionalFloat(name);

		case "BOOLEAN":
			return builder.optionalBoolean(name);

		case "BYTES":
			return builder.optionalBytes(name);

		default:
			throw new IllegalArgumentException("Unsupported type '" + type + "'");
		}
	}

	private static SchemaBuilder.FieldAssembler<Schema> closeFieldAssembler(
			SchemaBuilder.FieldAssembler<Schema> rootAssembler,
			SchemaBuilder.FieldAssembler<Schema> columnFieldsAssembler, String columnFamily) {

		if (columnFieldsAssembler == null)
			return rootAssembler;

		// add nested type to to root assembler
		return rootAssembler.name(columnFamily).type(columnFieldsAssembler.endRecord()).noDefault();
	}

	public static Schema buildSchema(SchemaMappingField[] schemaMappingFields) {
		// construct schema
		SchemaBuilder.FieldAssembler<Schema> rootAssembler = SchemaBuilder.record("root").fields();

		// note that the order needs to be exactly in-sync with the avro schema
		// generated on the MMLSpark/Scala side
		String lastColumnFamily = null;
		SchemaBuilder.FieldAssembler<Schema> columnFieldsAssembler = null;
		for (SchemaMappingField schemaMappingField : schemaMappingFields) {

			String columnFamily = schemaMappingField.getColumnFamily();
			String columnQualifier = schemaMappingField.getColumnQualifier();
			String type = schemaMappingField.getType();

			if (columnQualifier != null) {
				if (lastColumnFamily == null || !lastColumnFamily.equals(columnFamily)) {

					// close previous record
					rootAssembler = closeFieldAssembler(rootAssembler, columnFieldsAssembler, lastColumnFamily);

					// open new record
					columnFieldsAssembler = SchemaBuilder.record(columnFamily).fields();
				}

				// add the current field
				columnFieldsAssembler = addAvroField(columnFieldsAssembler, type, columnQualifier);
			} else {
				// close previous record
				rootAssembler = closeFieldAssembler(rootAssembler, columnFieldsAssembler, lastColumnFamily);
				columnFieldsAssembler = null;

				// add the top-level field
				rootAssembler = addAvroField(rootAssembler, type, columnFamily);
			}

			lastColumnFamily = columnFamily;
		}

		rootAssembler = closeFieldAssembler(rootAssembler, columnFieldsAssembler, lastColumnFamily);

		// setup serialization
		return rootAssembler.endRecord();
	}
}