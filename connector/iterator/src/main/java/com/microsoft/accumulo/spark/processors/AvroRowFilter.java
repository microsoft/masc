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

package com.microsoft.accumulo.spark.processors;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

import com.microsoft.accumulo.spark.juel.AvroELContext;
import com.microsoft.accumulo.spark.record.RowBuilderField;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Evaluates the user-supplied filter (JUEL syntax) against the constructed AVRO
 * record.
 * 
 * @implNote filter operates on AVRO Record object, not on the serialized
 *           version.
 */
public class AvroRowFilter extends AvroRowConsumer {
  private final static Logger logger = Logger.getLogger(AvroRowFilter.class);

  public static AvroRowFilter create(Map<String, String> options, String optionKey) {
    String filter = options.get(optionKey);

    return StringUtils.isEmpty(filter) ? null : new AvroRowFilter(filter, optionKey);
  }

  /**
   * Required for cloning.
   */
  private Schema schema;

  /**
   * Required for cloning.
   */
  private String filter;

  private String optionKey;

  /**
   * JUEL expression context exposing AVRO GenericRecord
   */
  private AvroELContext expressionContext;

  /**
   * JUEL filter expression
   */
  private ValueExpression filterExpression;

  private AvroRowFilter(String filter, String optionKey) {
    logger.info(optionKey + " filter '" + filter + "'");

    this.filter = filter;
    this.optionKey = optionKey;
  }

  @Override
  public String getName() {
    return super.getName() + " " + this.optionKey;
  }

  @Override
  protected boolean consumeInternal(Text rowKey, IndexedRecord record) throws IOException {
    // link AVRO record with JUEL expression context
    this.expressionContext.setCurrent(rowKey, record);

    return (boolean) filterExpression.getValue(this.expressionContext);
  }

  @Override
  public AvroRowConsumer clone() {
    AvroRowFilter copy = new AvroRowFilter(this.filter, this.optionKey);

    copy.initialize(schema);

    return copy;
  }

  @Override
  public Collection<RowBuilderField> getSchemaFields() {
    return Collections.<RowBuilderField>emptyList();
  }

  @Override
  public void initialize(Schema schema) {
    this.schema = schema;
    this.expressionContext = new AvroELContext(schema);

    ExpressionFactory factory = ExpressionFactory.newInstance();

    this.filterExpression = factory.createValueExpression(expressionContext, filter, boolean.class);
  }
}
