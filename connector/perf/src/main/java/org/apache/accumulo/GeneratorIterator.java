package org.apache.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class GeneratorIterator implements SortedKeyValueIterator<Key, Value> {
	private long rows;
	private Lorem lorem;
	private Key key;
	private Value value;
	private String cq;
	private String cf;
	private int minWords;
	private int maxWords;
	private long seed;

	public GeneratorIterator(long rows, long seed, String cf, String cq, int minWords, int maxWords) {
		this.rows = rows;
		this.seed = seed;
		this.cf = cf;
		this.cq = cq;
		this.minWords = minWords;
		this.maxWords = maxWords;
		this.lorem = new LoremIpsum(seed);
	}

	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		this.next();
	}

	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
	}

	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new GeneratorIterator(this.rows, this.seed, this.cq, this.cf, this.minWords, this.maxWords);
	}

	public boolean hasTop() {
		return rows > 0;
	}

	public void next() {
		this.rows--;
		this.key = new Key(String.format("key%010d", this.rows), this.cq, this.cq);
		this.value = new Value(this.lorem.getWords(this.minWords, this.maxWords));
	}

	public Key getTopKey() {
		return this.key;
	}

	public Value getTopValue() {
		return new Value();
	}
}
