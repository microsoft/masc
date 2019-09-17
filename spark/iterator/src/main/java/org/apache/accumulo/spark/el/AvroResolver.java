package org.apache.accumulo.spark.el;

import java.beans.FeatureDescriptor;
import java.util.Iterator;

import javax.el.ELContext;
import javax.el.ELException;
import javax.el.ELResolver;

public class AvroResolver extends ELResolver {

	@Override
	public Class<?> getCommonPropertyType(ELContext context, Object base) {
		System.out.println("getCommonPropertyType");
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<FeatureDescriptor> getFeatureDescriptors(ELContext context, Object base) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getType(ELContext context, Object base, Object property) {
		System.out.println("getType");
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(ELContext context, Object base, Object property) {
		// System.out.println("getValue " + base + "." + property + " = " +
		// ((AvroVariableDeferred) base).value());
		return null;
		// context.setPropertyResolved(true);
		// return ((AvroVariableDeferred) base).value();
	}

	@Override
	public boolean isReadOnly(ELContext context, Object base, Object property) {
		return true;
	}

	@Override
	public void setValue(ELContext context, Object base, Object property, Object value) {
		throw new ELException("setValue is not supported");
	}

}