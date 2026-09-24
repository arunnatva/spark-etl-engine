#
#
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("OracleConnection") \
        .config("spark.driver.extraJavaOptions","--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED") \
        .getOrCreate()


src_jdbc_url = "jdbc:oracle:thin:@tcp://awsbyinval0001.jnj.com:1521/DNV22001.jnj.com?ssl_server_dn_match=false"
#src_jdbc_url = "jdbc:oracle:thin:@tcps://itss-azdas-dv1.jdadelivers.com:2484/DVMK0WRV?ssl_server_dn_match=false"
#src_jdbc_url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=awsbyinval0001.jnj.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=DNV22001.jnj.com)))"
tgt_jdbc_url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=ITSUSRALSP04160.jnj.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=MRAQ0252.jnj.com)))"


source_properties = {
    "user": "ETH_CDLRPT",
    "password": "tHDb!T3OMb",
    "driver": "oracle.jdbc.OracleDriver"
}

target_properties = {
    "user": "SA_STG_CDL",
    "password": "#Power3003",
    "driver": "oracle.jdbc.OracleDriver"
}

src_df = spark.read.jdbc(
    url=src_jdbc_url,
    #table="LH_ETH_CDLRPT.MARKET_RPT",
    table="stg_cdl.GSC_FISCAL_CALENDAR",
    properties=source_properties
)


tgt_df = spark.read.jdbc(
    url=tgt_jdbc_url,
    table="stg_cdl.GSC_FISCAL_CALENDAR",
    properties=target_properties
)

#src_df.printSchema()
#tgt_df.printSchema()
#tgt_df = tgt_df.select('standard_date','jj_week_id')
print("********* table count *****", src_df.show(5))
print("********* table count *****", tgt_df.show(5))


#mydf = spark.read.parquet("s3a://edl-cdp-dev/consumer/vfeth/str/eth_working_dir/vflh_stg/css_manu_ing_fcst_stg")
#mydf.show()
#mydf.printSchema()
#print("********* file count ********", mydf.count())
