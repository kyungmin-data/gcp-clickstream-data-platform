import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, col

# -----------------------------
# 1. SparkSession with GCS & Kafka
# -----------------------------
spark = (
    SparkSession.builder
    .appName("kafka-consumer")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5",  # GCS 커넥터
        ])
    )
    .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "/opt/spark/secrets/km-598.json",
    )
    .getOrCreate()
)

# -----------------------------
# 2. Kafka & GCS config
# -----------------------------
BOOTSTRAP = "kafka:9092"
TOPIC = "clickstream_events"
GROUP_ID = "spark_clickstream_consumer_v2"

# Hive-style partition 디렉토리의 루트 경로
GCS_PATH = "gs://test-kafka-km/clickstream/raw"
GCS_CHECKPOINT = "gs://test-kafka-km/checkpoints/clickstream_v3"

# -----------------------------
# 3. Kafka offset: 3 hours ago (옵션)
# -----------------------------
three_hours_ago_ms = int((time.time() - 3 * 60 * 60) * 1000)

ts_json = json.dumps({
    TOPIC: {
        "0": three_hours_ago_ms
    }
})
print("startingOffsetsByTimestamp =", ts_json)

# -----------------------------
# 4. Read Kafka stream
# -----------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    # 운영 시: 특정 시점부터 읽고 싶으면 아래 옵션 사용
    # .option("startingOffsetsByTimestamp", ts_json)
    .option("startingOffsets", "earliest")   # 디버깅용: 토픽 전체 재처리
    .option("failOnDataLoss", "false")  # source: offset out-of-range 방지
    .option("kafka.group.id", GROUP_ID)
    .load()
)

# Kafka value = string JSON (아직 구조 파싱은 안 함)
value_df = raw_df.selectExpr("CAST(value AS STRING) AS value")

# -----------------------------
# 5. 시간(분) 단위 파티션 컬럼 추가 (processing time 기준)
# -----------------------------
with_ts = (
    value_df
    # 현재 Spark 처리 시각
    .withColumn("proc_ts", current_timestamp())
    # Hive-style partition 디렉토리로 쓸 컬럼들
    .withColumn("event_date",  date_format(col("proc_ts"), "yyyy-MM-dd"))
    .withColumn("event_hour",  date_format(col("proc_ts"), "HH"))
    .withColumn("event_minute", date_format(col("proc_ts"), "mm"))
)

# -----------------------------
# 6. Write to GCS as Parquet (Hive-style partition 디렉토리)
# -----------------------------
query = (
    with_ts.writeStream
    .format("parquet")
    .option("path", GCS_PATH)                        # 루트 경로
    .option("failOnDataLoss", "false")  # 데모 안정화
    .option("checkpointLocation", GCS_CHECKPOINT)    # 체크포인트
    .outputMode("append")
    .partitionBy("event_date", "event_hour", "event_minute")
    .queryName("clickstream_to_gcs_partitioned")
    .start()
)

query.awaitTermination()