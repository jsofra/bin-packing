# bin-packing

A example project using a bin packing algorithm to overcome data skew across partitions in a Spark Cluster, optimizing the work across the cluster.
A term-frequency inverse-document-frequency (tf-idf) algorithm is used as the work to optimize.


## Code

This repository contains the code for the bin-packing example.
The code is written in Clojure.

The main bin packing implementation can be seen at https://github.com/jsofra/bin-packing/blob/master/src/bin_packing/core.clj

The tf-idf implementation can be seen at https://github.com/jsofra/bin-packing/blob/master/src/bin_packing/tf_idf.clj

The implementation of the Spark custom partitioner can be seen at https://github.com/jsofra/bin-packing/blob/master/src/bin_packing/spark_example.clj#L29

Some spark utilities to provide a unified interface over both Spark RDD's and Clojure data structures can be seen at https://github.com/jsofra/bin-packing/blob/master/src/bin_packing/spark_utils.clj


## Presentation

You can see the slides for a presentation on bin-packing in Spark at https://jsofra.github.io/bin-packing-presentation/
