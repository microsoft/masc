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

package org.apache.accumulo.spark;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

public class AvroUtil {
  public static final Collection<ByteSequence> EMPTY_SET = new HashSet<>();

  public static GenericRecord deserialize(byte[] data, Schema schema) throws IOException {
    SpecificDatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

    return reader.read(null, decoder);
  }
}
