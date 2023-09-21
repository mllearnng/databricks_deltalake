from pyspark.sql.functions import lit
lines_to_skip = 3
lines_rdd = spark.sparkContext.textFile("/FileStore/tables/input1")
filtered_rdd = lines_rdd.zipWithIndex().filter(lambda x: x[1] >= lines_to_skip).map(lambda x: x[0])
df = spark.read.option("delimiter", ";").csv(filtered_rdd, header=True, inferSchema=True)
df_with_new_column = df.withColumn("year", lit(var_year))
display(df_with_new_column)
