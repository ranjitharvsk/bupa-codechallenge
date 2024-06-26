# bupa-coding

## Installation

``` bash
pip install pdm
```

## Usage

``` bash
pdm --help
```

## how to run test file

``` bash
python -m unittest exchange_assess_test.py
```

## how to run test file with coverage report

``` bash
python -m coverage run -m unittest exchange_assess_test.py
```

## Run coverage report just for  exchange_analysis.py
``` bash
coverage report -m exchange_analysis.py
```

# SUBMISSION
###  Documentation – Approach, architecture, best standard followed in the coding etc
* The Idea of creating this as a python project is to make it extensible and componetised, easy to test, deploy in the form of a data pipeline. 
* It can be integrated with any CI/CD.This can be deployed in any cloud environemnt or locally. 
* It can be configured in future to read and write to S3 buckets or any cloud data sources. 
* Instead of JSON it can be more effective and faster reading and writing to parquet files with date partitions.
* The job can be scheduled/Orchestrated to read the exchange rates from the raw zone date partitions as a AWS Glue job

### Coding Standard
* Cretaed a python project and used pdm for dependency management and unittest for testing inidvidual functions.
* Ingestion data source:  https://au.investing.com/currencies/aud-nzd-historical-data
* Chose to use pyspark to load, process, analyse the data 
    * Load: A json file saved locally was loaded using spark
            Input file is saved here: inputDir/exchange_rates.json'
    * Process: After file was successfully loaded data was   checked for the sanity and cleansed appropriately where required which includes
                missing records, deduping, nullchecks  and then all these information was logged.
    * Analyse: Min, Max, Cumulative average was calculated. 

## Code Snippets
Python file is located in src folder: src/rates_analysis/exchange_analysis.py
## Output results as appropritate
Summary is writtten as a CSV file to this location  'outputDir/summary_rates.csv'
## Testing scenario’s
Used unittest package  to test teh class file.
Coverage was also reported.
## Source files.
Git source: https://github.com/ranjitharvsk/bupa-codechallenge.git






