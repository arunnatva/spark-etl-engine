#
#
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("OracleConnection") \
        .config("spark.driver.extraJavaOptions","--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED") \
        .getOrCreate()


src_jdbc_url = "jdbc:oracle:thin:@tcps://itss-azdas-dv1.jdadelivers.com:2484/DVMK0WRV?ssl_server_dn_match=false"
#src_jdbc_url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=itss-azdas-dv1.jdadelivers.com)(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=DVMK0WRV)))"
tgt_jdbc_url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=itsusralsp08503.jnj.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=SRAD0764.jnj.com)))"


source_properties = {
    "user": "JNJDBUSER_RO",
    "password": "JNJDBUSER_RO123",
    "driver": "oracle.jdbc.OracleDriver"
}

target_properties = {
    "user": "MANUCTLTWRDEV",
    "password": "ManuCT123!dev",
    "driver": "oracle.jdbc.OracleDriver"
}


sql_stmt = """ SELECT UREP_ORDERNO, LENGTH(UREP_ORDERNO) LEN FROM JNJDBUSER_RO.SCHEDRCPTS WHERE LENGTH(UREP_ORDERNO) <= 30 """


src_df = spark.read.jdbc(
    url=src_jdbc_url,
    table="MANUGISTICS.SRC_CSS_RS_ORACLE_CAL",
    properties=source_properties
)

"""
src_df = (
    spark.read.format("jdbc")
    .option("url", src_jdbc_url)
    .option("dbtable", f"({sql_stmt}) SRC")
    .option("user", "JNJDBUSER_RO")
    .option("password","JNJDBUSER_RO123")
    .option("driver", "oracle.jdbc.OracleDriver")
    .load()
)
"""

tgt_df = spark.read.jdbc(
    url=tgt_jdbc_url,
    table="MANUCTLTWRDEV.ING_SNAP_SCHEDRCPTS",
    properties=target_properties
)

#src_df.printSchema()
#tgt_df.printSchema()
#tgt_df = tgt_df.select('standard_date','jj_week_id')
#tgt_df.createOrReplaceTempView("dim_dt")
#spark.sql("select * from dim_dt where standard_date = '09/05/2026' order by standard_date desc").show(10)
print("********* table count *****", src_df.show(5))
#print("********* table count *****", tgt_df.show(5))


#mydf = spark.read.parquet("s3a://edl-cdp-dev/consumer/vfeth/str/eth_working_dir/vflh_stg/css_manu_ing_fcst_stg")
#mydf.show()
#mydf.printSchema()
#print("********* file count ********", mydf.count())
