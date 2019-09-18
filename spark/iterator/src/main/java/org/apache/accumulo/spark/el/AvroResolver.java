package org.apache.accumulo.spark.el;

import java.beans.FeatureDescriptor;
import java.util.Arrays;
import java.util.Iterator;

import javax.el.ELContext;
import javax.el.ELException;
import javax.el.ELResolver;

public class AvroResolver extends ELResolver {

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
		throw new ELException("getType is not supported");
	}

	@Override
	public Object getValue(ELContext context, Object base, Object property) {
		throw new ELException("getValue is not supported");
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
			// TODO:
			context.setPropertyResolved(true);
			return Arrays.binarySearch(params, base) >= 0;
		}

		// System.out.println("Invoke: " + base + " + " + method);
		return null;
	}
}