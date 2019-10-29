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

import java.util.Arrays;

import javax.el.ELContext;
import javax.el.ELException;
import javax.el.ValueExpression;

import org.apache.accumulo.spark.juel.AvroELContext;
import org.apache.avro.generic.IndexedRecord;

/**
 * Expose Avro top-level record fields as JUEL ValueExpression
 */
public class AvroVariableExpression extends ValueExpression {
  private static final long serialVersionUID = 1L;

  private Class<?> type;
  // indices to walk through nested avro records
  private int[] fieldPositions;

  public AvroVariableExpression(Class<?> type, int... fieldPositions) {
    this.type = type;
    this.fieldPositions = fieldPositions;
  }

  @Override
  public Class<?> getExpectedType() {
    return type;
  }

  @Override
  public Class<?> getType(ELContext context) {
    return type;
  }

  @Override
  public Object getValue(ELContext context) {
    IndexedRecord record = ((AvroELContext) context).getAvroRecord();

    // supported nested records (e.g. column family/column qualifier)
    for (int i = 0; i < fieldPositions.length - 1; i++)
      record = (IndexedRecord) record.get(fieldPositions[i]);

    return record.get(fieldPositions[fieldPositions.length - 1]);
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

    AvroVariableExpression other = (AvroVariableExpression) obj;

    return type.equals(other.type) && Arrays.equals(fieldPositions, other.fieldPositions);
  }

  @Override
  public String getExpressionString() {
    throw new ELException("getExpressionString() is not supported");
  }

  @Override
  public int hashCode() {
    return type.hashCode() + Arrays.hashCode(fieldPositions);
  }

  @Override
  public boolean isLiteralText() {
    return false;
  }
}
