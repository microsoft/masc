package org.apache.accumulo.spark;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonGetter;

public class SchemaMappingField {

  private String columnFamily;
  private String columnQualifier;
  private String type;

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
