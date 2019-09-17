package org.apache.accumulo.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.el.*;
import de.odysseus.el.util.SimpleContext;
import junit.framework.TestCase;

import java.beans.FeatureDescriptor;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.accumulo.spark.el.AvroContext;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

public class ExpressionTest extends TestCase {

	public class MyELResolver extends ELResolver {
		@Override
		public Class<?> getCommonPropertyType(ELContext context, Object base) {
			System.out.println("getCommonPropertyType: " + base);
			return int.class;
		}

		@Override
		public Iterator<FeatureDescriptor> getFeatureDescriptors(ELContext context, Object base) {
			return null;
		}

		@Override
		public Class<?> getType(ELContext context, Object base, Object property) {
			System.out.println("getType: " + base + " and " + property);
			return int.class;
		}

		@Override
		public Object getValue(ELContext context, Object base, Object property) {
			System.out.println("getValue: " + base + " and " + property);

			context.setPropertyResolved(true);

			return 3;
			// if (property.equals("r")) {
			// }
			// return 2;
		}

		@Override
		public boolean isReadOnly(ELContext context, Object base, Object property) {
			return true;
		}

		@Override
		public void setValue(ELContext arg0, Object arg1, Object arg2, Object arg3) {
			throw new ELException("property is read-only");
		}

		@Override
		public Object invoke(ELContext context, Object base, Object method, Class<?>[] paramTypes, Object[] params) {
			if (base.getClass().equals(String.class) && method.equals("endsWith") && params.length == 1) {
				System.out.println("Invoke endsWith");
				context.setPropertyResolved(true);
				return ((String) base).endsWith((String) params[0]);
			} else if (method.equals("in")) {
				context.setPropertyResolved(true);
				return Arrays.binarySearch((Object[]) params[0], base) != -1;
			}

			System.out.println("Invoke: " + base + " + " + method);
			return null;
		}
	}

	private AvroContext context;
	private ExpressionFactory factory;
	private Schema schema;

	@Override
	public void setUp() throws Exception {
		factory = ExpressionFactory.newInstance();

		SchemaMappingField[] schemaMappingFields = new SchemaMappingField[] {
				new SchemaMappingField("cf1", "cq1", "long", "v0"),
				new SchemaMappingField("cf2", "cq2", "double", "v1") };

		schema = AvroUtil.buildSchema(schemaMappingFields);

		context = new AvroContext(schema, schemaMappingFields);
	}

	private void setRecordValues(long cq1, double cq2) {

		GenericRecordBuilder cf1RecordBuilder = new GenericRecordBuilder(schema.getField("cf1").schema());
		GenericRecordBuilder cf2RecordBuilder = new GenericRecordBuilder(schema.getField("cf2").schema());

		cf1RecordBuilder.set("cq1", cq1);
		cf2RecordBuilder.set("cq2", cq2);

		GenericRecordBuilder rootRecordBuilder = new GenericRecordBuilder(schema);
		rootRecordBuilder.set("cf1", cf1RecordBuilder.build());
		rootRecordBuilder.set("cf2", cf2RecordBuilder.build());

		context.setAvroRecord(rootRecordBuilder.build());
	}

	@Test
	public void testVariableExpressions() {

		ValueExpression exprV0 = factory.createValueExpression(context, "${v0}", long.class);

		// set the values after the expression is created
		setRecordValues(3L, 2.0);
		assertEquals(3L, exprV0.getValue(context));

		// test if we can reset it
		setRecordValues(4L, 2.5);
		assertEquals(4L, exprV0.getValue(context));

		// check for the second variable
		ValueExpression exprV1 = factory.createValueExpression(context, "${v1}", double.class);
		assertEquals(2.5, exprV1.getValue(context));
	}
}