import time

import pandas as pd
import pyarrow.csv as pc
import pyarrow.parquet as pq
from pyspark.sql import SparkSession


file_name = 'Building_Permits.csv'

# Pandas
tic = time.perf_counter()
pandas_df = pd.read_csv(file_name)
toc = time.perf_counter()
print(type(pandas_df))
print(f'Pandas read (CSV) in {toc - tic:0.4f} seconds')

# PySpark
spark = SparkSession.builder.master('local[1]').appName('MyApp').getOrCreate()

tic = time.perf_counter()
spark_df = spark.read.csv(file_name)
toc = time.perf_counter()
print(type(spark_df))
print(f'PySpark read (CSV) in {toc - tic:0.4f} seconds')

# Arrow
tic = time.perf_counter()
table = pc.read_csv(file_name)
df = table.to_pandas()
toc = time.perf_counter()
print(type(df))
print(f'Arrow read (CSV to Pandas) in {toc - tic:0.4f} seconds')

pq.write_table(table, 'building_permits.parquet')

tic = time.perf_counter()
pdf = pd.read_parquet('building_permits.parquet')
toc = time.perf_counter()
print(f'Pandas read (Parquet) in {toc - tic:0.4f} seconds')

tic = time.perf_counter()
table = pq.read_table('building_permits.parquet')
toc = time.perf_counter()
print(f'Arrow read (Parquet) in {toc - tic:0.4f} seconds')


# ----------

tic = time.perf_counter()
pdf = spark_df.toPandas()
toc = time.perf_counter()
print(type(pdf))
print(f'Spark DF to Pandas without Arrow in {toc - tic:0.4f} seconds')

# Got memory leak issue
# Set env `export ARROW_PRE_0_15_IPC_FORMAT=1` (Credit: https://george-jen.gitbook.io/data-science-and-apache-spark/enabling-for-conversion-to-from-pandas)
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')

tic = time.perf_counter()
pdf = spark_df.toPandas()
toc = time.perf_counter()
print(type(pdf))
print(f'Spark DF to Pandas using Arrow in {toc - tic:0.4f} seconds')
