/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.accumulo;

import java.util.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.data.Range;
import com.microsoft.accumulo.spark.processors.AvroRowMLeap;
import com.microsoft.accumulo.AvroRowEncoderIterator;
import com.google.common.io.Resources;
import org.apache.accumulo.core.data.ByteSequence;
import java.io.IOException;

public class MyBenchmark {
    public static final Collection<ByteSequence> EMPTY_SET = new HashSet<>();

    @Benchmark
    public void testMethod() throws IOException {
        // load mleap model
        byte[] mleapBundle = Resources.toByteArray(MyBenchmark.class.getResource("sentiment.zip"));
        String mleapBundleBase64 = Base64.getEncoder().encodeToString(mleapBundle);

        GeneratorIterator parentIterator = new GeneratorIterator((long) 1e5, 42, "text", "", 10, 15);
        AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

        Map<String, String> options = new HashMap<>();
        options.put(AvroRowEncoderIterator.SCHEMA, "[{\"cf\":\"text\",\"t\":\"string\"}]");

        // pass the model to the iterator
        options.put(AvroRowMLeap.MLEAP_BUNDLE, mleapBundleBase64);

        // if (StringUtils.isNotBlank(mleapFilter))
        // options.put(AvroRowEncoderIterator.MLEAP_FILTER, mleapFilter);

        parentIterator.init(null, null, null);
        iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
        iterator.seek(new Range(), EMPTY_SET, false);

        for (; iterator.hasTop(); iterator.next()) {
            Key key = iterator.getTopKey();
            Value value = iterator.getTopValue();
        }
    }
}
