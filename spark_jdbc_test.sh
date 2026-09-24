spark-submit \
--jars /opt/cloudera/parcels/CDH/jars/ojdbc8-21.3.0.0.jar \
--queue eth \
--conf spark.sql.queryExecutionListeners="" \
--conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED" \
--conf "spark.executor.extraJavaOptions=--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED" \
"$1"



#/home/anatva/eth_spark/src/spark_jdbc_test.py
