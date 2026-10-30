import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, count, max as smax, sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "smartcity_db")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "root")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")

# Construction de l'URL JDBC dynamiquement
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
PG_DRIVER = "org.postgresql.Driver"

# =========================================================
# SCHÉMAS JSON
# =========================================================
schema_transport = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("line", StringType(), True),
    StructField("city", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("delay_min", IntegerType(), True),
    StructField("ts", StringType(), True)
])

schema_air = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("co2", DoubleType(), True),
    StructField("ts", StringType(), True)
])

schema_incident = StructType([
    StructField("incident_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("type", StringType(), True),
    StructField("severity", IntegerType(), True),
    StructField("ts", StringType(), True)
])

# =========================================================
# SESSION SPARK
# =========================================================
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("SmartCity-Streaming")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.local.ip", "127.0.0.1")
    .config("spark.ui.port", "4050")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,"
        "org.postgresql:postgresql:42.7.7"
    )
    .getOrCreate()
)

print(f"⚡ Spark initialisé. Driver Host: {spark.conf.get('spark.driver.host')}")
spark.sparkContext.setLogLevel("WARN")

# =========================================================
# FONCTIONS UTILITAIRES D'ÉCRITURE POSTGRESQL
# =========================================================

def write_to_postgres_jdbc(batch_df, table_name):

    (batch_df.write
     .format("jdbc")
     .option("url", PG_URL)
     .option("dbtable", table_name)
     .option("user", PG_USER)
     .option("password", PG_PASSWORD)
     .option("driver", PG_DRIVER)
     .mode("append")
     .save())

def upsert_to_postgres(batch_df, table_name, pk_column, columns, sql_query):

    w = Window.partitionBy(pk_column).orderBy(col("event_time").desc())
    latest_rows = (batch_df
                   .withColumn("rn", row_number().over(w))
                   .filter(col("rn") == 1)
                   .select(*columns)
                   .collect())

    # 2. Se connecter à PostgreSQL
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()

        # 3. Exécuter l'UPSERT pour chaque ligne
        for row in latest_rows:
            cur.execute(sql_query, tuple(row))

        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erreur UPSERT sur {table_name}: {e}")

# =========================================================
# 1. LECTURE KAFKA & TRANSFORMATION
# =========================================================
def read_kafka_topic(topic, schema):
    """Lit un topic Kafka et le convertit en DataFrame structuré."""
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*")
            .withColumn("event_time", to_timestamp(col("ts")))
            .withWatermark("event_time", "10 seconds"))

# --- 1. TRANSPORT ---
df_transport = read_kafka_topic("transport_events", schema_transport) \
    .withColumn("is_late", (col("delay_min") > 5).cast("int"))

transport_kpi = (df_transport
                 .groupBy(window(col("event_time"), "10 seconds"), col("city"))
                 .agg(avg("speed").alias("avg_speed"),
                      sum("is_late").alias("late_count"))
                 .select(col("city"),
                         col("window.start").alias("window_start"),
                         col("window.end").alias("window_end"),
                         "avg_speed", "late_count"))

# --- 2. AIR QUALITY ---
df_air = read_kafka_topic("air_quality", schema_air) \
    .withColumn("is_polluted", ((col("pm25") > 35) | (col("co2") > 900)).cast("int"))

air_kpi = (df_air
           .groupBy(window(col("event_time"), "10 seconds"), col("city"))
           .agg(avg("pm25").alias("avg_pm25"),
                sum("is_polluted").alias("pollution_count"))
           .select(col("city"),
                   col("window.start").alias("window_start"),
                   col("window.end").alias("window_end"),
                   "avg_pm25", "pollution_count"))

# --- 3. INCIDENTS ---
df_incidents = read_kafka_topic("incidents", schema_incident) \
    .withColumn("is_severe", (col("severity") >= 3).cast("int"))

incidents_kpi = (df_incidents
                 .groupBy(window(col("event_time"), "10 seconds"), col("city"))
                 .agg(count("*").alias("incident_count"),
                      smax("severity").alias("max_severity"))
                 .select(col("city"),
                         col("window.start").alias("window_start"),
                         col("window.end").alias("window_end"),
                         "incident_count", "max_severity"))

# =========================================================
# 2. DÉFINITION DES REQUÊTES SQL D'UPSERT
# =========================================================
SQL_UPSERT_TRANSPORT = """
    INSERT INTO transport_latest(vehicle_id, line, city, lat, lon, speed, delay_min, event_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (vehicle_id) DO UPDATE SET
    line=EXCLUDED.line, city=EXCLUDED.city, lat=EXCLUDED.lat, lon=EXCLUDED.lon,
    speed=EXCLUDED.speed, delay_min=EXCLUDED.delay_min, event_time=EXCLUDED.event_time;
"""

SQL_UPSERT_AIR = """
    INSERT INTO air_latest(sensor_id, city, pm25, co2, event_time)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (sensor_id) DO UPDATE SET
    city=EXCLUDED.city, pm25=EXCLUDED.pm25, co2=EXCLUDED.co2, event_time=EXCLUDED.event_time;
"""

# =========================================================
# 3. DÉMARRAGE DES FLUX D'ÉCRITURE
# =========================================================
print("🚀 Démarrage des flux Spark...")

# --- Flux Transport ---
q_transport_latest = (df_transport.writeStream
                      .foreachBatch(lambda df, id: upsert_to_postgres(
                          df, "transport_latest", "vehicle_id",
                          ["vehicle_id", "line", "city", "lat", "lon", "speed", "delay_min", "event_time"],
                          SQL_UPSERT_TRANSPORT))
                      .outputMode("append")
                      .option("checkpointLocation", "C:/tmp/chk_transport_latest")
                      .start())

q_transport_kpi = (transport_kpi.writeStream
                   .foreachBatch(lambda df, id: write_to_postgres_jdbc(df, "transport_kpi_1min"))
                   .outputMode("append")
                   .option("checkpointLocation", "C:/tmp/chk_transport_kpi")
                   .start())

# --- Flux Air ---
q_air_latest = (df_air.writeStream
                .foreachBatch(lambda df, id: upsert_to_postgres(
                    df, "air_latest", "sensor_id",
                    ["sensor_id", "city", "pm25", "co2", "event_time"],
                    SQL_UPSERT_AIR))
                .outputMode("append")
                .option("checkpointLocation", "C:/tmp/chk_air_latest")
                .start())

q_air_kpi = (air_kpi.writeStream
             .foreachBatch(lambda df, id: write_to_postgres_jdbc(df, "air_kpi_1min"))
             .outputMode("append")
             .option("checkpointLocation", "C:/tmp/chk_air_kpi")
             .start())

# --- Flux Incidents ---
q_incidents_kpi = (incidents_kpi.writeStream
                   .foreachBatch(lambda df, id: write_to_postgres_jdbc(df, "incidents_kpi_1min"))
                   .outputMode("append")
                   .option("checkpointLocation", "C:/tmp/chk_incidents_kpi")
                   .start())

# Garder l'application active
spark.streams.awaitAnyTermination()