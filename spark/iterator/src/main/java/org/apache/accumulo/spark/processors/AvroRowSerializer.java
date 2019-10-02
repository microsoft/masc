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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroRowSerializer {
  // avro writer infra
  private ByteArrayOutputStream binaryBuffer = new ByteArrayOutputStream();
  private DatumWriter<IndexedRecord> writer;
  private BinaryEncoder encoder;

  public AvroRowSerializer(Schema schema) {
    this.writer = new SpecificDatumWriter<>(schema);
    this.encoder = EncoderFactory.get().binaryEncoder(binaryBuffer, null);
  }

  public byte[] serialize(IndexedRecord record) throws IOException {
    // make sure we're at the beginning again
    this.binaryBuffer.reset();

    // serialize the record
    this.writer.write(record, encoder);

    this.encoder.flush();
    this.binaryBuffer.flush();

    return this.binaryBuffer.toByteArray();
  }
}
