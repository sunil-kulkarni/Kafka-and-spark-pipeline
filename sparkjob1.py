from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, when, round, expr, unix_timestamp
from pyspark.sql.types import TimestampType

CPU_THRESHOLD = 83.0
MEM_THRESHOLD = 78.57

spark = SparkSession.builder.appName("CPU_MEM_Alert").getOrCreate()

cpu_df = spark.read.csv('./assets/cpu_data.csv', header=True, inferSchema=True)
mem_df = spark.read.csv('./assets/mem_data.csv', header=True, inferSchema=True)

combined_df = cpu_df.join(mem_df, on=['ts', 'server_id'])
combined_df = combined_df.withColumn('ts', col('ts').cast(TimestampType()))

combined_df = combined_df.withColumn(
    'ts_aligned', expr("timestamp_seconds(floor(unix_timestamp(ts)/10)*10)")
)

min_ts = combined_df.agg({'ts_aligned': 'min'}).collect()[0][0]
max_ts = combined_df.agg({'ts_aligned': 'max'}).collect()[0][0]
min_ts_unix, max_ts_unix = int(min_ts.timestamp()), int(max_ts.timestamp())

windowed_df = combined_df.groupBy(
    col('server_id'),
    window(col('ts_aligned'), '30 seconds', '10 seconds')
).agg(
    avg('cpu_pct').alias('avg_cpu'), avg('mem_pct').alias('avg_mem')
)

windowed_df = windowed_df.withColumn("window_start_ts", unix_timestamp("window.start"))
windowed_df = windowed_df.withColumn("window_end_ts", unix_timestamp("window.end"))

filtered_df = windowed_df.filter(
    (col("window_start_ts") >= min_ts_unix) &
    (col("window_end_ts") <= max_ts_unix) &
    ((col("window_start_ts") - min_ts_unix) % 10 == 0)
)

alert_df = filtered_df.withColumn(
    'alert',
    when((col('avg_cpu') > CPU_THRESHOLD) & (col('avg_mem') > MEM_THRESHOLD), "High CPU + Memory stress")
    .when((col('avg_cpu') > CPU_THRESHOLD), "CPU spike suspected")
    .when((col('avg_mem') > MEM_THRESHOLD), "Memory saturation suspected")
    .otherwise("")
)

final_df = alert_df.select(
    col('server_id'),
    col('window.start').alias('window_start'),
    col('window.end').alias('window_end'),
    round('avg_cpu', 2).alias('avg_cpu'),
    round('avg_mem', 2).alias('avg_mem'),
    col('alert')
).orderBy('server_id', 'window_start')

final_df.coalesce(1).write.csv('./assets/CPU_MEM.csv', header=True, mode='overwrite')
spark.stop()