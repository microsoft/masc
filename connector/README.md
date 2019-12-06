# Microsoft MASC, an Apache Spark connector for Apache Accumulo
[![Build Status](https://dev.azure.com/AZGlobal/Azure%20Global%20CAT%20Engineering/_apis/build/status/AGCE%20AI/Web%20Scale%20AI/microsoft.Accumulo?branchName=master)](https://dev.azure.com/AZGlobal/Azure%20Global%20CAT%20Engineering/_build/latest?definitionId=84&branchName=master) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.masc/microsoft-accumulo-spark-datasource/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.masc/microsoft-accumulo-spark-datasource)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.masc/microsoft-accumulo-spark-iterator/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.masc/microsoft-accumulo-spark-iterator)

This code provides connectivity between Apache Accumulo and Apache Spark.

## Main Goals
- Provide native Spark interface to connect to Accumulo
- Minimize data transfer between Spark and Accumulo
- Enable use of Machine Learning with Accumulo as the datastore

## Examples
```
# Read from Accumulo
df = (spark
      .read
      .format("com.microsoft.accumulo")
      .options(**options)  # define Accumulo properties
      .schema(schema))  # define schema for data retrieval

# Write to Accumulo
(df
 .write
 .format("org.apache.accumulo")
 .options(**options)
 .save())
```

See Pyspark [notebook](examples/AccumuloSparkConnector.ipynb) for a more detailed example.

## Capabilities
- Native Spark [Datasource V2](http://shzhangji.com/blog/2018/12/08/spark-datasource-api-v2/) API
- Row serialization using [Avro](https://avro.apache.org/)
- Filter pushdown (server-side)
- Expressive filter language using [JUEL](http://juel.sourceforge.net/)
- ML Inference pushdown (server-side) using [MLeap](http://mleap-docs.combust.ml/)
- Support Spark ML pipelines
- Minimal Java-runtime

## Installation

The connector is composed of two components:
- The [Datasource](datasource) component provides the interface used on the Spark side
- The [Iterator](iterator) component provides server-side functionality on the Accumulo side

The components can be built and test with Maven using Java 8+
```
mvn install
```

Once the targets have been built the following steps are needed:
1) Deploy iterator JAR to Accumulo lib folders on all nodes and restart the cluster
2) Add Datasource JAR in Spark
