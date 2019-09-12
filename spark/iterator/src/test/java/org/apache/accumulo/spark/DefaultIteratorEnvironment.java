package org.apache.accumulo.spark;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MapFileIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

// copied from https://github.com/apache/accumulo/blob/master/core/src/test/java/org/apache/accumulo/core/iterators/DefaultIteratorEnvironment.java
public class DefaultIteratorEnvironment implements IteratorEnvironment {

	AccumuloConfiguration conf;
	Configuration hadoopConf = new Configuration();

	public DefaultIteratorEnvironment(AccumuloConfiguration conf) {
		this.conf = conf;
	}

	public DefaultIteratorEnvironment() {
		this.conf = DefaultConfiguration.getInstance();
	}

	@Deprecated
	@Override
	public SortedKeyValueIterator<Key, Value> reserveMapFileReader(String mapFileName) throws IOException {
		FileSystem fs = FileSystem.get(hadoopConf);
		return new MapFileIterator(fs, mapFileName, hadoopConf);
	}

	@Override
	public boolean isSamplingEnabled() {
		return false;
	}
}