from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, when, to_timestamp, date_format, lit, round
from pyspark.sql.types import FloatType

# Thresholds
NET_IN_THRESHOLD = 4497.01
DISK_IO_THRESHOLD = 4434.43

# Start Spark session
spark = SparkSession.builder \
    .appName("Network_Disk_Alert") \
    .master("local[*]") \
    .getOrCreate()

# Read the two CSV files
net_df = spark.read.csv('./assets/net_data.csv', header=True, inferSchema=False)
disk_df = spark.read.csv('./assets/disk_data.csv', header=True, inferSchema=False)

# Filter malformed values
net_df = net_df.filter(col("net_in").rlike("^\d+(\.\d+)?$") & col("net_out").rlike("^\d+(\.\d+)?$"))
disk_df = disk_df.filter(col("disk_io").rlike("^\d+(\.\d+)?$"))

# Cast to FloatType
net_df = net_df.withColumn("net_in", col("net_in").cast(FloatType())) \
               .withColumn("net_out", col("net_out").cast(FloatType()))
disk_df = disk_df.withColumn("disk_io", col("disk_io").cast(FloatType()))

# Join and timestamp conversion
combined_df = net_df.join(disk_df, on=['ts', 'server_id']) \
    .withColumn('timestamp', to_timestamp(col('ts'), 'HH:mm:ss')) \
    .cache()

# Get time bounds
min_ts = combined_df.agg({"timestamp": "min"}).collect()[0][0]
max_ts = combined_df.agg({"timestamp": "max"}).collect()[0][0]

# Aggregate using max()
windowed_df = combined_df.groupBy(
    col('server_id'),
    window(col('timestamp'), windowDuration='30 seconds', slideDuration='10 seconds', startTime='0 seconds')
).agg(
    max(col('net_in')).alias('max_net_in'),
    max(col('disk_io')).alias('max_disk_io')
)

# Filter valid windows
final_windows = windowed_df.filter(
    (col('window.start') >= lit(min_ts)) &
    (col('window.start') < lit(max_ts))
)

# Apply alert logic
alert_df = final_windows.withColumn(
    'alert',
    when((col('max_net_in') > NET_IN_THRESHOLD) & (col('max_disk_io') > DISK_IO_THRESHOLD),
         "Network flood + Disk thrash suspected")
    .when((col('max_net_in') > NET_IN_THRESHOLD) & (col('max_disk_io') <= DISK_IO_THRESHOLD),
         "Possible DDoS")
    .when((col('max_disk_io') > DISK_IO_THRESHOLD) & (col('max_net_in') <= NET_IN_THRESHOLD),
         "Disk thrash suspected")
    .otherwise("")
)

# Final selection
final_df = alert_df.select(
    col('server_id'),
    date_format(col('window.start'), 'HH:mm:ss').alias('window_start'),
    date_format(col('window.end'), 'HH:mm:ss').alias('window_end'),
    round(col('max_net_in'), 2).alias('max_net_in'),
    round(col('max_disk_io'), 2).alias('max_disk_io'),
    col('alert')
).orderBy(['server_id', 'window_start'])

# Save output
final_df.coalesce(1).write.mode('overwrite').option("header", "true").csv('./assets/team_25_NET_DISK.csv')

print("Output saved to ./assets/NET_DISK.csv")
print(f"Total rows generated: {final_df.count()}")
final_df.show(5)

spark.stop()
