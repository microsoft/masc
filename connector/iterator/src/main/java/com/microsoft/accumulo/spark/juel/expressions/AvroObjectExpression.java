/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.accumulo.spark.juel.expressions;

import javax.el.ELContext;
import javax.el.ELException;
import javax.el.ValueExpression;

import com.microsoft.accumulo.spark.juel.AvroELContext;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;

/**
 * JUEL VariableExpression resolving to a nested record.
 */
public class AvroObjectExpression extends ValueExpression {

  private static final long serialVersionUID = 1L;

  private Field field;

  public AvroObjectExpression(Field field) {
    this.field = field;
  }

  @Override
  public Class<?> getExpectedType() {
    return Record.class;
  }

  @Override
  public Class<?> getType(ELContext context) {
    return Record.class;
  }

  @Override
  public Object getValue(ELContext context) {
    IndexedRecord record = ((AvroELContext) context).getAvroRecord();

    return record.get(this.field.pos());
  }

  @Override
  public boolean isReadOnly(ELContext context) {
    return true;
  }

  @Override
  public void setValue(ELContext context, Object value) {
    throw new ELException("setValue not supported");
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AvroVariableExpression))
      return false;

    AvroObjectExpression other = (AvroObjectExpression) obj;

    return this.field.equals(other.field);
  }

  @Override
  public String getExpressionString() {
    throw new ELException("getExpressionString() is not supported");
  }

  @Override
  public int hashCode() {
    return this.field.hashCode();
  }

  @Override
  public boolean isLiteralText() {
    return false;
  }
}
