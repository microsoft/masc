111: createtable table1
112: insert row1 cola colb value1
113: insert row2 cold cole value2
114: scan
115: setiter -n dup -class org.apache.accumulo.spark.DuplicationIterator -t table1 -p 10 -majc
116: 15
117: scan
118: du
119: compact
120: scan
121: du
122: deleteiter -n dup -majc