from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import threading
import psycopg2
from psycopg2.extras import execute_batch
from urllib.parse import urlparse
import os
import tempfile
import shutil

spark = SparkSession.builder \
    .appName("StableCryptoAnalytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
    .config("spark.sql.shuffle.partitions", "4") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

os.environ['NEON_DB_URL'] = (
    'postgresql://neondb_owner:npg_7JBAyP1fwxnp@'
    'ep-snowy-resonance-a22qpjwe-pooler.eu-central-1.aws.neon.tech/neondb'
    '?sslmode=require&channel_binding=require'
)
db_params = urlparse(os.environ['NEON_DB_URL'])

crypto_schema = StructType([
    StructField("E", LongType(), True),
    StructField("s", StringType(), True),
    StructField("c", StringType(), True),
    StructField("o", StringType(), True),
    StructField("h", StringType(), True),
    StructField("l", StringType(), True),
    StructField("v", StringType(), True),
    StructField("q", StringType(), True),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "100") \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), crypto_schema).alias("data")) \
    .select(
        col("data.E").alias("event_time_ms"),
        col("data.s").alias("symbol"),
        col("data.c").cast("double").alias("close_price"),
        col("data.o").cast("double").alias("open_price"),
        col("data.h").cast("double").alias("high_price"),
        col("data.l").cast("double").alias("low_price"),
        col("data.v").cast("double").alias("volume"),
        col("data.q").cast("double").alias("quote_volume")
    ) \
    .withColumn("event_time", (col("event_time_ms") / 1000).cast("timestamp")) \
    .withColumn("price_change", col("close_price") - col("open_price")) \
    .withColumn("price_change_pct", (col("price_change") / col("open_price")) * 100) \
    .drop("event_time_ms")

volume_state = {}

def write_to_postgres(table_name, rows):
    if not rows:
        return
    conn, cursor = None, None
    try:
        conn = psycopg2.connect(
            dbname=db_params.path[1:],
            user=db_params.username,
            password=db_params.password,
            host=db_params.hostname,
            port=db_params.port,
            sslmode='require'
        )
        cursor = conn.cursor()

        if table_name == "binance_prices":
            query = """
            INSERT INTO binance_prices (event_time, symbol, price, volume, volume_delta)
            VALUES (%s, %s, %s, %s, %s)
            """
        elif table_name == "volume_analysis":
            query = """
            INSERT INTO volume_analysis
            (symbol, window_start, window_end, total_volume, total_quote_volume, avg_price, trades_count, avg_trade_size)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
        elif table_name == "market_metrics":
            query = """
            INSERT INTO market_metrics
            (symbol, window_start, window_end, avg_volatility, vwap, total_volume, total_vwap)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        execute_batch(cursor, query, rows)
        conn.commit()
    except Exception as e:
        print(f"❌ Error writing {table_name}: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def handle_prices(batch_df, epoch_id):
    global volume_state
    if batch_df.isEmpty():
        return

    rows = []
    for row in sorted(batch_df.collect(), key=lambda x: (x.symbol, x.event_time)):
        symbol = row.symbol
        current_vol = float(row.volume)
        delta = current_vol - volume_state.get(symbol, 0.0)
        volume_state[symbol] = current_vol
        rows.append((row.event_time, symbol, float(row.close_price), current_vol, delta))

    write_to_postgres("binance_prices", rows)

def handle_volume_analysis(batch_df, epoch_id):
    if batch_df.isEmpty():
        return
    rows = [
        (row.symbol, row.window.start, row.window.end, row.total_volume,
         row.total_quote_volume, row.avg_price, row.trades_count, row.avg_trade_size)
        for row in batch_df.collect()
    ]
    write_to_postgres("volume_analysis", rows)

def handle_market_metrics(batch_df, epoch_id):
    if batch_df.isEmpty():
        return
    rows = [
        (row.symbol, row.window.start, row.window.end, row.avg_volatility,
         row.vwap, row.total_volume, row.total_vwap)
        for row in batch_df.collect()
    ]
    write_to_postgres("market_metrics", rows)

def start_streams():
    queries = []

    checkpoint_dir = os.path.join(tempfile.gettempdir(), "spark_checkpoint")
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
    os.makedirs(checkpoint_dir, exist_ok=True)

    # --- Real-time Prices ---
    queries.append(
        df_parsed.writeStream
        .foreachBatch(handle_prices)
        .outputMode("update")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "prices"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    # --- Volume Analysis ---
    volume_analysis_df = df_parsed \
        .withWatermark("event_time", "1 minute") \
        .groupBy("symbol", window("event_time", "10 minutes", "2 minutes")) \
        .agg(
            sum("volume").alias("total_volume"),
            sum("quote_volume").alias("total_quote_volume"),
            avg("close_price").alias("avg_price"),
            count("*").alias("trades_count")
        ) \
        .withColumn("avg_trade_size", col("total_quote_volume") / col("trades_count"))

    queries.append(
        volume_analysis_df.writeStream
        .foreachBatch(handle_volume_analysis)
        .outputMode("update")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "volume_analysis"))
        .trigger(processingTime="1 minute")
        .start()
    )

    # --- Market Metrics ---
    market_metrics_df = df_parsed \
        .withColumn("price_volatility_score", abs(col("price_change_pct"))) \
        .withColumn("volume_weighted_price", col("close_price") * col("volume")) \
        .groupBy("symbol", window("event_time", "15 minutes", "5 minutes")) \
        .agg(
            avg("price_volatility_score").alias("avg_volatility"),
            sum("volume_weighted_price").alias("total_vwap"),
            sum("volume").alias("total_volume")
        ) \
        .withColumn("vwap", col("total_vwap") / col("total_volume"))

    queries.append(
        market_metrics_df.writeStream
        .foreachBatch(handle_market_metrics)
        .outputMode("update")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "market_metrics"))
        .trigger(processingTime="3 minutes")
        .start()
    )

    return queries

def display_realtime():
    while True:
        try:
            conn = psycopg2.connect(
                dbname=db_params.path[1:],
                user=db_params.username,
                password=db_params.password,
                host=db_params.hostname,
                port=db_params.port,
                sslmode='require'
            )
            cursor = conn.cursor()

            print(f"\n=== REAL-TIME PRICES ({time.strftime('%Y-%m-%d %H:%M:%S')}) ===")
            cursor.execute("""
                SELECT event_time, symbol, price, volume, volume_delta
                FROM binance_prices
                ORDER BY event_time DESC LIMIT 5
            """)
            rows = cursor.fetchall()
            if rows:
                print(f"{'Time':<20} {'Symbol':<12} {'Price':<15} {'Volume':<15} {'Delta':<15}")
                print("-" * 80)
                for row in rows:
                    print(f"{row[0]} {row[1]} {row[2]:.8f} {row[3]:.8f} {row[4]:.8f}")
            else:
                print("No data yet...")

            cursor.close()
            conn.close()
            time.sleep(3)
        except Exception as e:
            print(f"Display error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    try:
        queries = start_streams()

        display_thread = threading.Thread(target=display_realtime)
        display_thread.daemon = True
        display_thread.start()

        queries[0].awaitTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
        spark.stop()
