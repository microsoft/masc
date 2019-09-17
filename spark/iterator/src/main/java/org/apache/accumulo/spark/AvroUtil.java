package org.apache.accumulo.spark;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class AvroUtil {
	private static SchemaBuilder.FieldAssembler<Schema> addColumnQualifierFields(
			SchemaBuilder.FieldAssembler<Schema> builder, List<SchemaMappingField> fields) {
		for (SchemaMappingField schemaMappingField : fields) {
			switch (schemaMappingField.getType().toUpperCase()) {
			case "STRING":
				builder = builder.optionalString(schemaMappingField.getColumnQualifier());
				break;

			case "LONG":
				builder = builder.optionalLong(schemaMappingField.getColumnQualifier());
				break;

			case "INTEGER":
				builder = builder.optionalInt(schemaMappingField.getColumnQualifier());
				break;

			case "DOUBLE":
				builder = builder.optionalDouble(schemaMappingField.getColumnQualifier());
				break;

			case "FLOAT":
				builder = builder.optionalFloat(schemaMappingField.getColumnQualifier());
				break;

			case "BOOLEAN":
				builder = builder.optionalBoolean(schemaMappingField.getColumnQualifier());
				break;

			case "BYTES":
				builder = builder.optionalBytes(schemaMappingField.getColumnQualifier());
				break;

			default:
				throw new IllegalArgumentException("Unsupported type '" + schemaMappingField.getType() + "'");
			}
		}

		return builder;
	}

	public static Schema buildSchema(SchemaMappingField[] schemaMappingFields) {
		// construct schema
		// List<Schema.Field> fields = new ArrayList<>();
		SchemaBuilder.FieldAssembler<Schema> rootAssembler = SchemaBuilder.record("root").fields();

		// group fields by column family
		Map<String, List<SchemaMappingField>> groupedByColumnFamily = Stream.of(schemaMappingFields)
				.collect(Collectors.groupingBy(SchemaMappingField::getColumnFamily));

		// loop over column families
		for (Map.Entry<String, List<SchemaMappingField>> entry : groupedByColumnFamily.entrySet()) {

			// loop over column qualifiers
			SchemaBuilder.FieldAssembler<Schema> columnFieldsAssembler = SchemaBuilder.record(entry.getKey()).fields();
			columnFieldsAssembler = addColumnQualifierFields(columnFieldsAssembler, entry.getValue());

			// add nested type to to root assembler
			rootAssembler = rootAssembler.name(entry.getKey()).type(columnFieldsAssembler.endRecord()).noDefault();
		}

		// setup serialization
		return rootAssembler.endRecord();
	}
}