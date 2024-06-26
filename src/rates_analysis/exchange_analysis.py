import findspark
findspark.init()

import pyspark
from pyspark import SparkContext,SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import os.path
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_format,to_date,round,format_number
from pyspark.sql import Row
from datetime import datetime,timedelta
import pandas as pd


class ExchangeAnalysis:

    def __init__(self):
        # get spark session initialised.
        
        APP_NAME = "Bupa: ASSESSMENT"
        try:
            self.spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
            
        except RuntimeError:
            print("Exception occurred.")

    def read_file(self,path):
        ex_rates = self.spark.read.json(path)
        ex_rates.printSchema()

        ex_rates.createOrReplaceTempView("ex_rates_vw")

        target_df = self.spark.sql("select * from ex_rates_vw ")
        # target_df.show()
        return target_df
    

    def find_missingdates(self,p_df):
        # Last 30 days dates form today
        start_date = datetime.today().date()-timedelta(days=30)

        dateseq_list = pd.date_range(start_date, periods=30, freq='D').to_list()
        pandas_df = pd.DataFrame(dateseq_list)
        pandas_df = pandas_df.rename(columns={pandas_df.columns[0]: 'Seqdate_name'}) 
        pandas_df['Seqdate_name_str'] = pd.to_datetime(pandas_df['Seqdate_name']).dt.strftime('%Y-%m-%d')
        pandas_list = pandas_df['Seqdate_name_str'].tolist()

        spark_column_df = p_df.select('Date','date_column')
        spark_column_df = spark_column_df.withColumn('date_new', date_format(col('date_column'), 'yyyy-MM-dd'))
        # Spark_list = spark_column_df.select('date_new').rdd.flatMap(lambda x: x).collect()
        
        sparkdt_list = []
        for i in spark_column_df.collect():
            sparkdt_list.append(i[2])
        
        # print(sparkdt_list) missing dates rates form the list
        set1 = set(sparkdt_list)
        set2 = set(pandas_list)
        missing_in_list1 = list(set2 - set1)
        return missing_in_list1
    
    def dedupe_records(self,p_df):
        p_df.createOrReplaceTempView("dedupes_vw")
        dedupes_df = self.spark.sql("SELECT distinct * FROM dedupes_vw")
        
        return dedupes_df
    
    def validate_dupes(self,p_df):
        p_df.createOrReplaceTempView("dupes_vw")
        dupes_df = self.spark.sql("SELECT Date, COUNT(Date) FROM dupes_vw GROUP BY Date HAVING COUNT(Date) >1")
        return dupes_df.count()
        
    def validate_nullchecks(self,p_df):
        p_df.createOrReplaceTempView("nullchecks_vw")
        nc_df = self.spark.sql("select * from nullchecks_vw where (`Change_%` IS NULL) or (`Date` IS NULL) or (`Price` IS NULL) ")
        # nc_df.show()
        return nc_df
    
    
                
    def process(self,p_in_path,p_out_path):
        

        data_df = self.read_file(p_in_path)
        
        data_df = data_df.withColumn("date_column", to_date(col("Date"), "dd/MM/yyyy"))

        # Get the missing rates dates
        missing_rates = self.find_missingdates(data_df)


        dup_records = self.validate_dupes(data_df)
        if dup_records > 0:
            data_df1 =self.dedupe_records(data_df)

        else:
            data_df1 =self.dedupe_records(data_df)

        null_records = self.validate_nullchecks(data_df1)
        if null_records.count() > 0:
            # null_records.show() #null values can be handled at the time of saving the file
            null_records.na.drop()

        data_df1.createOrReplaceTempView("select_vw")
        summary_df = self.spark.sql("""
                                    Select a.* ,
                                    CASE WHEN a.Price = (select Max(b.Price) from select_vw b)then "Max Price"
                                         WHEN a.Price = (select Min(b.Price) from select_vw b) then "Min Price"
                                    Else
                                        "N/A"
                                    End as Max_Min,
                                    round(avg(a.Price) OVER (ORDER BY date_column ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),4) as CumulativeAvg
                                   from select_vw a

                                   """)
        
        
        # summary_df.show()
        
        summary_df.write.csv(p_out_path, header=True, mode='overwrite')
        # self.spark.stop()
        return summary_df
    
    
    
# test = ExchangeAnalysis()
# EX_RATES_FILE = "inputDir/exchange_rates.json"
# dir = os.path.abspath(os.getcwd())
# in_path = os.path.join(dir, EX_RATES_FILE)
# out_path = os.path.join(dir, "outputDir/summary_rates.csv")
# df = test.process(in_path,out_path)

