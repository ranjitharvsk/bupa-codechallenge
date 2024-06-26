import findspark
findspark.init()

import pyspark
from pyspark.sql.session import SparkSession


spark = SparkSession.builder.appName("APP_NAME").getOrCreate()

df_st = spark.read.csv("/Users/krayakota/development/projects/bupa-coding/src/rates_analysis/inputDir/exchange_rate_30days1.csv",header = True)
df_st.show()

df_st.write.mode("overwrite").json("/Users/krayakota/development/projects/bupa-coding/src/rates_analysis/inputDir/exchange_rates_new1.json")
path = "/Users/krayakota/development/projects/bupa-coding/src/rates_analysis/inputDir/exchange_rates_new1.json"
exgrates = spark.read.json(path)

exgrates.printSchema()

exgrates.createOrReplaceTempView("exgrates_v")

target_df = spark.sql("select * from exgrates_v ")
target_df.show()
print(target_df.count())

