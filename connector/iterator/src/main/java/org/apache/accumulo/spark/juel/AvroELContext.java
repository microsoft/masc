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

package org.apache.accumulo.spark.juel;

import javax.el.ELContext;
import javax.el.ELResolver;
import javax.el.FunctionMapper;
import javax.el.VariableMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.io.Text;

/**
 * Exposes Avro GenericRecord as Expression Language (EL) context for filtering
 * and column computation.
 */
public class AvroELContext extends ELContext {

  private IndexedRecord avroRecord;
  private Text rowKey;
  private VariableMapper variableMapper;
  private ELResolver resolver;

  public AvroELContext(Schema schema) {
    variableMapper = new AvroVariableMapper(schema);
    resolver = new AvroResolver();
  }

  @Override
  public ELResolver getELResolver() {
    return resolver;
  }

  @Override
  public FunctionMapper getFunctionMapper() {
    return null;
  }

  @Override
  public VariableMapper getVariableMapper() {
    return variableMapper;
  }

  public IndexedRecord getAvroRecord() {
    return avroRecord;
  }

  public Text getRowKey() {
    return rowKey;
  }

  public void setCurrent(Text rowKey, IndexedRecord avroRecord) {
    this.rowKey = rowKey;
    this.avroRecord = avroRecord;
  }
}
