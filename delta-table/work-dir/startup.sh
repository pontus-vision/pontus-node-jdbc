:#!/usr/bin/bash

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab --ip=0.0.0.0'


cd $SPARK_HOME/work-dir

../sbin/start-thriftserver.sh  \
--jars "$(ls /opt/spark/jars/*|xargs|sed -e 's/ /,/g' )" \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--hiveconf "hive.input.format=io.delta.hive.HiveInputFormat" \
--hiveconf "hive.tez.input.format=io.delta.hive.HiveInputFormat"


$SPARK_HOME/bin/pyspark --packages io.delta:${DELTA_PACKAGE_VERSION} \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--conf "spark.ssl.enabled=true" \
--conf "spark.ssl.protocol=TLS" \
--conf "spark.ssl.keyStore=/opt/spark/work-dir/certs/keystore.jks" \
--conf "spark.ssl.keyStorePassword=pa55word" \
--conf "spark.ssl.keyPassword=pa55word" \
--conf "spark.ssl.keyStoreType=JKS"



#--hiveconf "hive.server2.use.SSL=true" \
#--hiveconf "hive.server2.keystore.path=/opt/spark/work-dir/certs/keystore.jks" \
#--hiveconf "hive.server2.keystore.password=pa55word" \
#--hiveconf "hive.server2.thrift.http.path=/"  \
#--hiveconf "hive.server2.transport.mode=http" \
#--hiveconf "hive.server2.thrift.http.port=10000" \
#--hiveconf "hive.server2.http.endpoint=/" \
  # --packages 'io.delta:delta-core_2.12:1.0.0'
#  --packages io.delta:${DELTA_PACKAGE_VERSION}

