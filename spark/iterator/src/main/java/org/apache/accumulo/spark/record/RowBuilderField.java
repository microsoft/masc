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

package org.apache.accumulo.spark.record;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * POJO for the user-supplied schema fields.
 */
public class RowBuilderField {
  private String columnFamily;
  private String columnQualifier;
  private String type;
  private String filterVariableName;
  private boolean nullable = true;

  public RowBuilderField() {
  }

  public RowBuilderField(String columnFamily, String columnQualifier, String type, String filterVariableName) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    this.type = type;
    this.filterVariableName = filterVariableName;
  }

  public RowBuilderType getRowBuilderType() {
    return RowBuilderType.valueOfIgnoreCase(this.type);
  }

  /**
   * @return the nullable
   */
  public boolean isNullable() {
    return nullable;
  }

  /**
   * @param nullable the nullable to set
   */
  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  /**
   * @param filterVariableName the filterVariableName to set
   */
  @JsonSetter(value = "fvn")
  public void setFilterVariableName(String filterVariableName) {
    this.filterVariableName = filterVariableName;
  }

  /**
   * @return the filterVariableName
   */
  @JsonGetter(value = "fvn")
  public String getFilterVariableName() {
    return filterVariableName;
  }

  /**
   * @param columnFamily the columnFamily to set
   */
  @JsonSetter(value = "cf")
  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
  }

  /**
   * @return the columnFamily
   */
  @JsonGetter(value = "cf")
  public String getColumnFamily() {
    return columnFamily;
  }

  /**
   * @return the columnQualifier
   */
  @JsonGetter(value = "cq")
  public String getColumnQualifier() {
    return columnQualifier;
  }

  /**
   * @param columnQualifier the columnQualifier to set
   */
  @JsonSetter(value = "cq")
  public void setColumnQualifier(String columnQualifier) {
    this.columnQualifier = columnQualifier;
  }

  /**
   * @return the type
   */
  @JsonGetter(value = "t")
  public String getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  @JsonSetter(value = "t")
  public void setType(String type) {
    this.type = type;
  }
}
