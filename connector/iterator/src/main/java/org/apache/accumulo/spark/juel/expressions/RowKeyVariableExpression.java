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

package org.apache.accumulo.spark.juel.expressions;

import javax.el.ELContext;
import javax.el.ELException;
import javax.el.ValueExpression;

import org.apache.accumulo.spark.juel.AvroELContext;
import org.apache.hadoop.io.Text;

/**
 * JUEL VariableExpression resolving to the row key.
 */
public class RowKeyVariableExpression extends ValueExpression {

  public static final RowKeyVariableExpression INSTANCE = new RowKeyVariableExpression();

  private static final long serialVersionUID = 1L;

  @Override
  public Class<?> getExpectedType() {
    return String.class;
  }

  @Override
  public Class<?> getType(ELContext context) {
    return String.class;
  }

  @Override
  public Object getValue(ELContext context) {
    Text text = ((AvroELContext) context).getRowKey();

    return text.toString();
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
    return obj instanceof RowKeyVariableExpression;
  }

  @Override
  public String getExpressionString() {
    throw new ELException("getExpressionString() is not supported");
  }

  @Override
  public int hashCode() {
    return 42;
  }

  @Override
  public boolean isLiteralText() {
    return false;
  }
}
