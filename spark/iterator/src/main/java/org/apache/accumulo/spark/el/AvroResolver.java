package org.apache.accumulo.spark.el;

import java.beans.FeatureDescriptor;
import java.util.Arrays;
import java.util.Iterator;

import javax.el.ELContext;
import javax.el.ELException;
import javax.el.ELResolver;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

public class AvroResolver extends ELResolver {

	private static Class<?> avroTypeToJavaType(Field field) {
		Type type = field.schema().getType();

		if (type == Type.BOOLEAN)
			return boolean.class;
		else if (type == Type.DOUBLE)
			return double.class;
		else if (type == Type.FLOAT)
			return float.class;
		else if (type == Type.INT)
			return int.class;
		else if (type == Type.LONG)
			return long.class;
		else
			throw new IllegalArgumentException("Unsupported type: " + type);
	}

	@Override
	public Class<?> getCommonPropertyType(ELContext context, Object base) {
		throw new ELException("getCommonPropertyType is not supported");
	}

	@Override
	public Iterator<FeatureDescriptor> getFeatureDescriptors(ELContext context, Object base) {
		return null;
	}

	@Override
	public Class<?> getType(ELContext context, Object base, Object property) {
		return avroTypeToJavaType(((Record) base).getSchema().getField((String) property));
	}

	@Override
	public Object getValue(ELContext context, Object base, Object property) {
		Record record = (Record) base;

		context.setPropertyResolved(true);

		return record.get((String) property);
	}

	@Override
	public boolean isReadOnly(ELContext context, Object base, Object property) {
		return true;
	}

	@Override
	public void setValue(ELContext context, Object base, Object property, Object value) {
		throw new ELException("setValue is not supported");
	}

	@Override
	public Object invoke(ELContext context, Object base, Object method, Class<?>[] paramTypes, Object[] params) {
		if (base.getClass().equals(String.class) && params.length == 1) {

			String baseStr = (String) base;
			String paramStr = (String) params[0];

			if (method.equals("endsWith")) {
				context.setPropertyResolved(true);
				return baseStr.endsWith(paramStr);
			}

			if (method.equals("startsWith")) {
				context.setPropertyResolved(true);
				return baseStr.startsWith(paramStr);
			}

			if (method.equals("contains")) {
				context.setPropertyResolved(true);
				return baseStr.contains(paramStr);
			}

		} else if (method.equals("in")) {
			context.setPropertyResolved(true);
			return Arrays.binarySearch(params, base) >= 0;
		}

		return null;
	}
}