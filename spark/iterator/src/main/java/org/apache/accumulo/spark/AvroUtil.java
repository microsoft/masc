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

	public static Schema buildSchema(SchemaMappingField[] schemaMappingFields) {
		// construct schema
		// List<Schema.Field> fields = new ArrayList<>();
		SchemaBuilder.FieldAssembler<Schema> rootAssembler = SchemaBuilder.record("root").fields();

		// group fields by column family
		Map<String, List<SchemaMappingField>> groupedByColumnFamily = Stream.of(schemaMappingFields)
				.collect(Collectors.groupingBy(SchemaMappingField::getColumnFamily));

		// loop over column families
		for (Map.Entry<String, List<SchemaMappingField>> entry : groupedByColumnFamily.entrySet()) {

			List<SchemaMappingField> schemaMappingFieldsPerFamily = entry.getValue();
			SchemaMappingField firstField = schemaMappingFieldsPerFamily.get(0);

			if (schemaMappingFieldsPerFamily.size() > 1 || firstField.getColumnQualifier() != null) {
				// loop over column qualifiers
				SchemaBuilder.FieldAssembler<Schema> columnFieldsAssembler = SchemaBuilder.record(entry.getKey())
						.fields();
				for (SchemaMappingField schemaMappingField : schemaMappingFieldsPerFamily)
					columnFieldsAssembler = addAvroField(columnFieldsAssembler, schemaMappingField.getType(),
							schemaMappingField.getColumnQualifier());

				// add nested type to to root assembler
				rootAssembler = rootAssembler.name(entry.getKey()).type(columnFieldsAssembler.endRecord()).noDefault();
			} else {

				rootAssembler = addAvroField(rootAssembler, firstField.getType(), firstField.getColumnFamily());
			}
		}

		// setup serialization
		return rootAssembler.endRecord();
	}
}