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

package org.apache.accumulo.spark.processors;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.spark.record.RowBuilderField;
import org.apache.accumulo.spark.util.StopWatch;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.io.Text;

public abstract class AvroRowConsumer {

  // private StopWatch stopWatchConsume = new StopWatch();

  /**
   * Process the row.
   * 
   * @param rowKey The row key.
   * @param record The AVRO record.
   * @return The same or a new processed record. Null if processing should be
   *         stopped (e.g. does not match a filter).
   */
  public boolean consume(Text rowKey, IndexedRecord record) throws IOException {
    // this.stopWatchConsume.start();

    boolean ret = this.consumeInternal(rowKey, record);
    // if (ret)
      // this.stopWatchConsume.stop();

    return ret;
  }

  // public double getAverageConsumeTime() {
  //   return this.stopWatchConsume.getAverage();
  // }

  public String getName() {
    return getClass().getSimpleName();
  }

  /**
   * Process the row.
   * 
   * @param rowKey The row key.
   * @param record The AVRO record.
   * @return The same or a new processed record. Null if processing should be
   *         stopped (e.g. does not match a filter).
   */
  protected abstract boolean consumeInternal(Text rowKey, IndexedRecord record) throws IOException;

  /**
   * Support copying of the object as the iterator needs to be copyable.
   * 
   * @return The cloned object.
   */
  public abstract AvroRowConsumer clone();

  /**
   * Any additional fields this consumer wants to populate.
   * 
   * @return additional fields added to the main schema.
   */
  public abstract Collection<RowBuilderField> getSchemaFields();

  /**
   * Final initialization of the consumer wants the entire schema was discovered.
   * 
   * @param schema The final schema.
   */
  public abstract void initialize(Schema schema);
}
