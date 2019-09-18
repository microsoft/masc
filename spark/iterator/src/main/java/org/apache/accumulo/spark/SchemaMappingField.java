package org.apache.accumulo.spark;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonGetter;

public class SchemaMappingField {

  private String columnFamily;
  private String columnQualifier;
  private String type;
  private String filterVariableName;

  public SchemaMappingField() {
  }

  public SchemaMappingField(String columnFamily, String columnQualifier, String type, String filterVariableName) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    this.type = type;
    this.filterVariableName = filterVariableName;
  }

  public Class<?> getJavaType() {
    switch (getType().toUpperCase()) {
    case "STRING":
      return String.class;

    case "LONG":
      return long.class;

    case "INTEGER":
      return int.class;

    case "DOUBLE":
      return double.class;

    case "FLOAT":
      return float.class;

    case "BOOLEAN":
      return boolean.class;

    // case "BYTES":
    default:
      return null;
    }
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
