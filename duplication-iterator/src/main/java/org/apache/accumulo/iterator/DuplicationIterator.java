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

package org.apache.accumulo.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;

public class DuplicationIterator implements SortedKeyValueIterator<Key, Value>, OptionDescriber {
	public static final String OPTION_COUNT = "count";
	public static final String OPTION_DELIMITER = "delimiter";
	// TODO
	// proposed option & feature
	// - check if delimiter exists in row key
	// - try to parse duplication count after delimiter
	// - if the number of digits matches the current number, skip row
	//
	// * pro: avoid unwanted execution
	// * cons: can't distinguish between count 20 and 50, since it has the same
	// number of digits
	// private static final String OPTION_CHECK_DELIMITER = "check_delimiter";

	private SortedKeyValueIterator<Key, Value> source;
	private int targetDuplicationCount;
	private String keyFormat;

	private Key topKey;
	private Value topValue;
	private int currentDuplicationIndex;
	private String delimiter;

	private Iterator<Pair<Key, Value>> keysIterator;
	private ArrayList<Pair<Key, Value>> keys = new ArrayList<>();

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		this.source = source;
		this.targetDuplicationCount = Integer.parseInt(options.getOrDefault(OPTION_COUNT, "1"));
		this.delimiter = options.getOrDefault(OPTION_DELIMITER, "_");

		int digits = (int) (Math.log10(targetDuplicationCount) + 1);
		this.keyFormat = "%s%s%0" + digits + "d";
	}

	@Override
	public boolean hasTop() {
		return topKey != null;
	}

	private void prepKeys() throws IOException {
		if (keysIterator != null) {
			// current iterator still has values
			if (keysIterator.hasNext()) {
				setTop(keysIterator.next());
				return;
			}

			// should we duplicate more?
			if (currentDuplicationIndex < targetDuplicationCount) {
				currentDuplicationIndex++;

				keysIterator = keys.iterator();
				setTop(keysIterator.next());

				return;
			}

			keys.clear();
		}

		do {
			if (!source.hasTop())
				return;

			Text currentRow = new Text(source.getTopKey().getRow());

			while (source.hasTop() && source.getTopKey().getRow().equals(currentRow)) {
				// collect all cells
				keys.add(new Pair<Key, Value>(new Key(source.getTopKey()), new Value(source.getTopValue())));

				// move forward
				source.next();
			}
		} while (keys.isEmpty());

		currentDuplicationIndex = 0;

		keysIterator = keys.iterator();
		setTop(keysIterator.next());

		// ### source data
		// row key, column family, column qualifier, value
		// abc, cf1, cq1, v1 -> abc00, abc01, ... abc09
		// abc, cf2, cq1, v2
		// def, cf1, cq1, v3

		// ### output data
		// abc, cf1, cq1, v1 <--
		// abc, cf2, cq1, v2 <--
		// abc00, cf1, cq1, v1 <-- dups start
		// abc00, cf2, cq1, v2
	}

	@Override
	public void next() throws IOException {
		topKey = null;
		topValue = null;

		prepKeys();
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		this.source.seek(range, columnFamilies, inclusive);

		prepKeys();
	}

	private void setTop(Pair<Key, Value> top) {
		// form new key by appending currentDuplicationIndex
		String row = top.getFirst().getRow().toString();

		this.topKey = new Key(new Text(String.format(keyFormat, row, delimiter, currentDuplicationIndex)),
				top.getFirst().getColumnFamily(), top.getFirst().getColumnQualifier());
		this.topValue = top.getSecond();
	}

	@Override
	public Key getTopKey() {
		return topKey;
	}

	@Override
	public Value getTopValue() {
		return topValue;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		DuplicationIterator copy = new DuplicationIterator();
		copy.source = source;
		copy.targetDuplicationCount = targetDuplicationCount;
		copy.keyFormat = keyFormat;

		// skipping current

		return copy;
	}

	@Override
	public IteratorOptions describeOptions() {
		HashMap<String, String> namedOptions = new HashMap<>();
		namedOptions.put(OPTION_COUNT, "Number of duplicates rows to produce");
		namedOptions.put(OPTION_DELIMITER, "Delimiter to use between row key and duplication index");

		return new IteratorOptions(getClass().getSimpleName(),
				"Duplicates values by appending an index to each row key", namedOptions, null);
	}

	@Override
	public boolean validateOptions(Map<String, String> options) {
		String count = options.get(OPTION_COUNT);
		if (count != null) {
			try {
				Integer.parseInt(count);
			} catch (NumberFormatException ne) {
				return false;
			}
		}

		return true;
	}
}