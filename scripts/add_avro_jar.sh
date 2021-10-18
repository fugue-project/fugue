#!/bin/bash

SPARK_JAR=$(pip show pyspark | grep Location | cut -d' ' -f 2)
wget -U "Any User Agent" -O "${SPARK_JAR}/pyspark/jars/spark-avro_2.11-3.2.0.jar"  https://repo1.maven.org/maven2/com/databricks/spark-avro_2.11/3.2.0/spark-avro_2.11-3.2.0.jar
