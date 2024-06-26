import os
import unittest
from rates_analysis.exchange_analysis import ExchangeAnalysis
from pyspark.sql.functions import col, date_format,to_date
from pyspark.sql import functions as F
import coverage


class TestConsumer(unittest.TestCase):

    def setUp(self):

        self.test = ExchangeAnalysis()
        self.dir = os.path.abspath(os.getcwd())
        
    def test_read_file(self):

        # exchange_rates_test_read_file.json
        path = os.path.join(self.dir,"resources/exchange_rates_test_read_file.json")

        target_df = self.test.read_file(path)
        assert target_df.count() == 5

    def test_find_missingdates(self):
        path = os.path.join(self.dir,"resources/exchange_rates_test_find_missingdates.json")

        data_df = self.test.read_file(path)
        data_df = data_df.withColumn("date_column", to_date(col("Date"), "dd/MM/yyyy"))
        missing_list = self.test.find_missingdates(data_df)
        assert len(missing_list) ==  26

    def test_dedupe_records(self):
        path = os.path.join(self.dir,"resources/exchange_rates_test_dedupe_records.json")
        data_df = self.test.read_file(path)
        data_df = data_df.withColumn("date_column", to_date(col("Date"), "dd/MM/yyyy"))
                                     
        dup_records = self.test.dedupe_records(data_df)
        assert dup_records.count() == 4 # after deduping test file 4 unique records exists

    def test_validate_dupes(self):
        path = os.path.join(self.dir,"resources/exchange_rates_test_dedupe_records.json")
        data_df = self.test.read_file(path)
        data_df = data_df.withColumn("date_column", to_date(col("Date"), "dd/MM/yyyy"))

        dup_records = self.test.validate_dupes(data_df)
        assert dup_records == 3

    def test_validate_nullchecks(self):
        path = os.path.join(self.dir,"resources/exchange_rates_test_validate_nullchecks.json")
        data_df = self.test.read_file(path)
        data_df = data_df.withColumn("date_column", to_date(col("Date"), "dd/MM/yyyy"))

        nullcheck_df = self.test.validate_nullchecks(data_df)
        assert nullcheck_df.count() ==3 

    def test_process(self):
        in_path = os.path.join(self.dir,"resources/exchange_rates_test_process.json")
        out_path = "resources/summary_rates_test.csv"
        

        process_df = self.test.process(in_path,out_path)
        assert process_df.agg(F.max(process_df['Price'])).collect()[0][0] == "1.0877"
        assert process_df.agg(F.min(process_df['Price'])).collect()[0][0] == "1.0855"

    

if __name__ == '__main__':
    
    cov = coverage.Coverage()
    cov.start()

    unittest.main()

    cov.stop()
    cov.save()

    cov.html_report()
    print("Done.")

