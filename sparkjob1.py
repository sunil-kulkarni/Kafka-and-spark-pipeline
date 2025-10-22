from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum, count, when, round, to_timestamp, date_format, lit
from pyspark.sql.types import FloatType

# Thresholds
CPU_THRESHOLD = 83.0
MEM_THRESHOLD = 78.57

# Start Spark session
spark = SparkSession.builder \
    .appName("CPU_MEM_Alert_Final_Accuracy") \
    .master("local[*]") \
    .getOrCreate()

# Read CSVs
cpu_df = spark.read.csv('./assets/cpu_data.csv', header=True, inferSchema=False)
mem_df = spark.read.csv('./assets/mem_data.csv', header=True, inferSchema=False)

# Filter malformed values
cpu_df = cpu_df.filter(col("cpu_pct").rlike("^\d+(\.\d+)?$"))
mem_df = mem_df.filter(col("mem_pct").rlike("^\d+(\.\d+)?$"))

# Cast to FloatType
cpu_df = cpu_df.withColumn("cpu_pct", col("cpu_pct").cast(FloatType()))
mem_df = mem_df.withColumn("mem_pct", col("mem_pct").cast(FloatType()))

# Join and timestamp conversion
combined_df = cpu_df.join(mem_df, on=['ts', 'server_id']) \
    .withColumn('timestamp', to_timestamp(col('ts'), 'HH:mm:ss')) \
    .cache()

# Get time bounds
min_ts = combined_df.agg({"timestamp": "min"}).collect()[0][0]
max_ts = combined_df.agg({"timestamp": "max"}).collect()[0][0]

# Aggregate using sum/count
windowed_df = combined_df.groupBy(
    col('server_id'),
    window(col('timestamp'), windowDuration='30 seconds', slideDuration='10 seconds', startTime='0 seconds')
).agg(
    (sum(col('cpu_pct')) / count(col('cpu_pct'))).alias('avg_cpu'),
    (sum(col('mem_pct')) / count(col('mem_pct'))).alias('avg_mem')
)

# Filter valid windows
final_windows = windowed_df.filter(
    (col('window.start') >= lit(min_ts)) &
    (col('window.start') < lit(max_ts))
)

# Apply descriptive alert logic
alert_df = final_windows.withColumn(
    'alert',
    when((col('avg_cpu') >= CPU_THRESHOLD) & (col('avg_mem') >= MEM_THRESHOLD),
         "High CPU + Memory stress")
    .when((col('avg_cpu') >= CPU_THRESHOLD) & (col('avg_mem') < MEM_THRESHOLD),
          "CPU spike suspected")
    .when((col('avg_mem') >= MEM_THRESHOLD) & (col('avg_cpu') < CPU_THRESHOLD),
          "Memory saturation suspected")
    .otherwise("")
)

# Final selection with rounded numeric values (kept as floats)
final_df = alert_df.select(
    col('server_id'),
    date_format(col('window.start'), 'HH:mm:ss').alias('window_start'),
    date_format(col('window.end'), 'HH:mm:ss').alias('window_end'),
    round(col('avg_cpu'), 2).alias('avg_cpu'),
    round(col('avg_mem'), 2).alias('avg_mem'),
    col('alert')
).orderBy(['server_id', 'window_start'])

# Save output
final_df.coalesce(1).write.mode('overwrite').option("header", "true").csv('./assets/team_25_CPU_MEM.csv')

print("Output saved to ./assets/team_25_CPU_MEM.csv")
print(f"Total rows generated: {final_df.count()}")
final_df.show(5)

spark.stop()
