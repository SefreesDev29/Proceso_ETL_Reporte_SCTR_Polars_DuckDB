import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField, LongType
from delta.tables import DeltaTable

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

BASE_PATH = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
PATH_LAKE = BASE_PATH / 'DataLake_Local'
TEMP_DIR = Path("E:/Spark_Temp") 
TEMP_DIR.mkdir(parents=True, exist_ok=True)
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

spark = (
    SparkSession.builder 
    .appName("Auditar_ETL_Medallion_Emision")
    .master("local[*]")
    .config("spark.driver.memory", "24g")
    .config("spark.local.dir", TEMP_DIR.as_posix())
    #.config("spark.sql.ansi.enabled", "false")
    # .config("spark.sql.parquet.compression.codec", "zstd")
    # .config("spark.sql.session.timeZone", "America/Lima")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .getOrCreate()
)   

spark.conf.set("spark.sql.session.timeZone", "America/Lima")
spark.sql("SELECT current_timestamp()").show(truncate=False)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")

print(f"🚩 Entorno Databricks: {IS_DATABRICKS}")
if IS_DATABRICKS:
    # spark.conf.set("spark.sql.shuffle.partitions", "200") 
    spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "true")
    
else:
    spark.conf.set("spark.sql.shuffle.partitions", "10") 

path_tabla = f"{PATH_LAKE}/Gold/Consolidado_Emision"
dt = DeltaTable.forPath(spark, path_tabla)

historia_df = dt.history()
historia_df.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

spark.sql(f"DESCRIBE DETAIL delta.`{PATH_LAKE}/Gold/Consolidado_Emision`").show()

df_log = spark.read.json(f"{path_tabla}/_delta_log/*.json")

df_files = df_log.filter(col("add").isNotNull()).select("add.*")

schema_stats = StructType([
    StructField("minValues", StructType([
        StructField("ULT_DIGI_DOC", LongType(), True),
        StructField("NUM_DOC", StringType(), True)
    ]), True),
    StructField("maxValues", StructType([
        StructField("ULT_DIGI_DOC", LongType(), True),
        StructField("NUM_DOC", StringType(), True)
    ]), True),
    StructField("numRecords", LongType(), True)
])

df_indices = df_files.withColumn("stats_parsed", from_json(col("stats"), schema_stats)) \
                     .select(
                         col("path").alias("archivo_parquet"),
                         col("stats_parsed.numRecords").alias("filas"),
                         col("stats_parsed.minValues.ULT_DIGI_DOC").alias("min_ult_digi_doc"),
                         col("stats_parsed.maxValues.ULT_DIGI_DOC").alias("max_ult_digi_doc"),
                         col("stats_parsed.minValues.NUM_DOC").alias("min_doc"),
                         col("stats_parsed.maxValues.NUM_DOC").alias("max_doc")
                     )

print("📊 ESTADÍSTICAS DE DATA SKIPPING (Z-ORDER RESULT):")
df_indices.orderBy("min_ult_digi_doc").show(20, truncate=False)
sys.exit(0)