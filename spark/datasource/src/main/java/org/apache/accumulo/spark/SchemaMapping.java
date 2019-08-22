package org.apache.accumulo.spark;

import java.util.Map;

public class SchemaMapping {
  private String rowKeyTargetColumn;
  private Map<String,SchemaMappingField> mapping;

  /**
   * @return the rowKeyTargetColumn
   */
  public String getRowKeyTargetColumn() {
    return rowKeyTargetColumn;
  }

  /**
   * @param rowKeyTargetColumn
   *          the rowKeyTargetColumn to set
   */
  public void setRowKeyTargetColumn(String rowKeyTargetColumn) {
    this.rowKeyTargetColumn = rowKeyTargetColumn;
  }

  /**
   * @return the mapping
   */
  public Map<String,SchemaMappingField> getMapping() {
    return mapping;
  }

  /**
   * @param mapping
   *          the mapping to set
   */
  public void setMapping(Map<String,SchemaMappingField> mapping) {
    this.mapping = mapping;
  }
}
