
#!/usr/bin/env bash

set -euo pipefail

fugue_spark_version="${FUGUE_SPARK_VERSION:-3.5.8}"
if [[ ! "${fugue_spark_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Invalid Spark version: ${fugue_spark_version}" >&2
    exit 1
fi

case "${fugue_spark_version%%.*}" in
    3) scala_binary_version="2.12" ;;
    4) scala_binary_version="2.13" ;;
    *)
        echo "Unsupported Spark version: ${fugue_spark_version}" >&2
        exit 1
        ;;
esac

spark_distribution="spark-${fugue_spark_version}-bin-hadoop3"
wget -qO- \
    "https://archive.apache.org/dist/spark/spark-${fugue_spark_version}/${spark_distribution}.tgz" \
    | tar -xz -C /tmp
# export SPARK_NO_DAEMONIZE=1
bash "/tmp/${spark_distribution}/sbin/start-connect-server.sh" \
    --jars "https://repo1.maven.org/maven2/org/apache/spark/spark-connect_${scala_binary_version}/${fugue_spark_version}/spark-connect_${scala_binary_version}-${fugue_spark_version}.jar"
