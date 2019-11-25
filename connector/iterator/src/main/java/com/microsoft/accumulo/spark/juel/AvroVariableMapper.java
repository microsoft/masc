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

package com.microsoft.accumulo.spark.juel;

import java.util.HashMap;
import java.util.Map;

import javax.el.ValueExpression;
import javax.el.VariableMapper;

import com.microsoft.accumulo.spark.juel.expressions.AvroObjectExpression;
import com.microsoft.accumulo.spark.juel.expressions.AvroVariableExpression;
import com.microsoft.accumulo.spark.juel.expressions.RowKeyVariableExpression;
import com.microsoft.accumulo.spark.record.AvroSchemaBuilder;
import com.microsoft.accumulo.spark.record.RowBuilderType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

/**
 * Resolve JUEL variables against Avro schema.
 */
public class AvroVariableMapper extends VariableMapper {

  private static final String ROWKEY_VARIABLE_NAME = "rowKey";

  private Schema schema;

  /**
   * fast lookup for variable names modelled by Avro aliases.
   */
  private Map<String, AvroVariableExpression> aliasMap;

  public AvroVariableMapper(Schema schema) {
    this.schema = schema;

    // build alias to VariableExpression map
    this.aliasMap = new HashMap<>();
    for (Field field : schema.getFields()) {

      if (field.schema().getType() == Type.RECORD) {
        for (Field nestedField : field.schema().getFields()) {
          // find the corresponding java class
          Class<?> nestedFieldClass = RowBuilderType.valueOf(nestedField.getProp(AvroSchemaBuilder.ROWBUILDERTYPE_PROP))
              .getJavaClass();

          for (String alias : nestedField.aliases())
            this.aliasMap.put(alias, new AvroVariableExpression(nestedFieldClass, field.pos(), nestedField.pos()));
        }
      } else {
        // find the corresponding java class
        Class<?> fieldClass = RowBuilderType.valueOf(field.getProp(AvroSchemaBuilder.ROWBUILDERTYPE_PROP))
            .getJavaClass();
        for (String alias : field.aliases())
          this.aliasMap.put(alias, new AvroVariableExpression(fieldClass, field.pos()));
      }
    }
  }

  /**
   * Resolve variables in this order: rowKey, mapped variables (e.g. v2 = cf1.cq1)
   * and finally using variable expressions.
   */
  @Override
  public ValueExpression resolveVariable(String variable) {
    if (variable.equals(ROWKEY_VARIABLE_NAME))
      return RowKeyVariableExpression.INSTANCE;

    // check if this is a statically resolved variable (e.g. v2 = cf1.cq1)
    AvroVariableExpression expr = this.aliasMap.get(variable);

    // otherwise default to dynamic lookup
    return expr != null ? expr : new AvroObjectExpression(this.schema.getField(variable));
  }

  @Override
  public ValueExpression setVariable(String variable, ValueExpression expression) {
    return null;
  }
}
