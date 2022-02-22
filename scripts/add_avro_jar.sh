#!/bin/bash

SPARK_JAR=$(pip show pyspark | grep Location | cut -d' ' -f 2)
wget -U "Any User Agent" -O "${SPARK_JAR}/pyspark/jars/spark-avro_2.12-3.2.1.jar"  https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.2.1/spark-avro_2.12-3.2.1.jar
