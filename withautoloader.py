from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from delta.tables import *
from pyspark.sql.functions import lit

#auto loader approah 
def read_from_incremental_to_raw_logs(raw_logs_table_name):
  query  = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "anylocation")
      .load("your sourceloa")
      .writeStream
      .option("checkpointLocation", "s3://heb-dsol-core-stg-qlik-poc/sreekanth_trails/autoloader")
      .option("mergeSchema", "true")
      .trigger(once=True)
      .table(raw_logs_table_name)  #this table would be created automatically 
      .awaitTermination())
  return query;  


query = read_from_incremental_to_raw_logs(raw_logs_table_name)

#read raw logs
logs_df = spark.table(raw_logs_table_name)
display(logs_df)

#getting the year of the data 


var_first_line = logs_df.first()[0]
var_year = var_first_line.split(":")[1]
print(var_year)

# getting the 

lines_to_skip = 3

filtered_rdd = logs_df.zipWithIndex().filter(lambda x: x[1] >= lines_to_skip).map(lambda x: x[0])
df = spark.read.option("delimiter", ";").csv(filtered_rdd, header=True, inferSchema=True)
df_with_new_column = df.withColumn("year", lit(var_year))
display(df_with_new_column)
