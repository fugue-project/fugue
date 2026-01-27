wget https://dlcdn.apache.org/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz -O - | tar -xz -C /tmp
# export SPARK_NO_DAEMONIZE=1
bash /tmp/spark-4.1.1-bin-hadoop3/sbin/start-connect-server.sh --jars https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/4.1.1/spark-connect_2.12-4.1.1.jar
