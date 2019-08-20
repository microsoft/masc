package org.apache.accumulo.spark;

public class SchemaMappingField {
  private String columnFamily;
  private String columnQualifier;
  private String type;

  /**
   * @param columnFamily
   *          the columnFamily to set
   */
  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
  }

  /**
   * @return the columnFamily
   */
  public String getColumnFamily() {
    return columnFamily;
  }

  /**
   * @return the columnQualifier
   */
  public String getColumnQualifier() {
    return columnQualifier;
  }

  /**
   * @param columnQualifier
   *          the columnQualifier to set
   */
  public void setColumnQualifier(String columnQualifier) {
    this.columnQualifier = columnQualifier;
  }

  /**
   * @return the type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type
   *          the type to set
   */
  public void setType(String type) {
    this.type = type;
  }
}
