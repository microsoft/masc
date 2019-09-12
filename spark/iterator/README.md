
# Package Installation

```bash
cd spark
cd iterator

mvn package
cp target/accumulo-spark-avro-1.0.0-SNAPSHOT.jar $ACCUMULO_HOME/lib/accumulo-spark-avro-1.0.0-SNAPSHOT.jar

# this is required when using with spark/datasource v2 connector
cp ~/.m2/repository/org/apache/avro/avro/1.9.0/avro-1.9.0.jar $ACCUMULO_HOME/lib/

./bin/uno stop accumulo

# wait a bit... check with 'ps aux...' if there are left overs
# ps aux | grep java
./bin/uno start accumulo
```

# Demo

```bash
createtable table1
insert row1 cola colb value1
insert row2 cold cole value2
scan
setiter -n dup -class org.apache.accumulo.spark.DuplicationIterator -t table1 -p 10 -majc
_
15
scan
du
compact
scan
du
deleteiter -n dup -majc
```
