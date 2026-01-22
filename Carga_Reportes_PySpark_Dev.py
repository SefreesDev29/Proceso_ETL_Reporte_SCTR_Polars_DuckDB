from datetime import datetime
import shutil
import sys
import os
from pathlib import Path
import fastexcel 
import pandas as pd 
from loguru import logger
from rich import print

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, StringType, StructType, StructField, LongType
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

TEMP_DIR = Path("E:/Spark_Temp_SCTR") 
TEMP_DIR.mkdir(parents=True, exist_ok=True)

HORA_INICIAL, HORA_FINAL = datetime.now(), datetime.now()
# PERIODO = str(HORA_INICIAL.year) + str(HORA_INICIAL.month).zfill(2) + str(HORA_INICIAL.day).zfill(2)
# PERIODO = f"{HORA_INICIAL.year}{HORA_INICIAL.month:02d}{HORA_INICIAL.day:02d}"
PERIODO = HORA_INICIAL.date()

BASE_PATH = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
PATH_SOURCE_EXP = BASE_PATH / 'Reportes_Expuestos'
PATH_SOURCE_CONT = BASE_PATH / 'Reportes_Contratantes'
PATH_LAKE = BASE_PATH / 'DataLake_Local'
PATH_LOG = BASE_PATH / 'Logs' / f'LogAppPS_{PERIODO}.log'

COLS_IDX_EXP = [1,2,3,5,6,7,8,9,10,11,12,13,18,19]
COLS_NAM_EXP = ['POLIZA','F_INI_VIGEN_POLIZA','F_FIN_VIGEN_POLIZA',
                'CERTIFICADO','F_INI_COBERT','F_FIN_COBERT',
                'P_NOMBRE','S_NOMBRE','AP_PATERNO','AP_MATERNO',
                'TIPO_DOC','NUM_DOC','YEAR_MOV','MONTH_MOV']
COLS_NAM_EXP_FINAL = ['POLIZA','F_INI_VIGEN_POLIZA','F_FIN_VIGEN_POLIZA',
                'CERTIFICADO','F_INI_COBERT','F_FIN_COBERT',
                'TIPO_DOC','NUM_DOC','ULT_DIGI_DOC','EXPUESTO',
                'YEAR_MOV','MONTH_MOV','FECHA_CARGA']

COLS_IDX_CONT = [1,2,3,6,8,9]
COLS_NAM_CONT = ['TIPO_DOC','NUM_DOC_CONT','CONTRATANTE','POLIZA','YEAR_MOV','MONTH_MOV']
COLS_NAM_CONT_FINAL = ['POLIZA','TIPO_DOC','NUM_DOC_CONT','CONTRATANTE','YEAR_MOV','MONTH_MOV','FECHA_CARGA']

    
def custom_format(type_process: int):
    def formatter(record: dict):
        levelname = record['level'].name
        if levelname == 'INFO':
            text = 'AVISO'
            level_str = f'<cyan>{text:<7}</cyan>'
            message_color = '<cyan>'
        elif levelname == 'WARNING':
            text = 'ALERTA'
            level_str = f'<level>{text:<7}</level>'
            message_color = '<level>'
        elif levelname == 'SUCCESS':
            text = 'ÉXITO'
            level_str = f'<level>{text:<7}</level>'
            message_color = '<level>'
        else:
            level_str = f'<level>{levelname:<7}</level>'
            message_color = '<level>'
        
        original_message = str(record['message'])
        # safe_message = original_message.replace("{", "{{").replace("}", "}}")
        safe_message = (original_message
                .replace("{", "{{")
                .replace("}", "}}")
                .replace("<", "\\<")
                .replace(">", "\\>")
               )
        custom_message = f"{message_color}{safe_message}</{message_color.strip('<>')}>\n"
        
        if type_process == 0:
            level_str = f'{level_str} | '
        else:
            level_str = f"{level_str} | {record['name']}:{record['function']}:{record['line']} - "
            if record["exception"] is not None:
                custom_message += f"{record['exception']}\n"

        return (
            f"<cyan><bold>{record['time']:DD/MM/YYYY HH:mm:ss}</bold></cyan> | "
            f"{level_str}"
            f"{custom_message}"
        )
    return formatter

def remove_log():
    logger.remove()

def add_log_console():
    logger.add(sys.stdout,
            backtrace=False, diagnose=False, level='DEBUG',
            colorize=True,
            format=custom_format(0))

def add_log_file(exits_log: bool):
    global FILE_LOG_EXISTS
    if PATH_LOG.exists() and not exits_log:
        logger.add(PATH_LOG, 
                backtrace=True, diagnose=True, level='DEBUG',
                format='\n\n{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
        return
    
    logger.add(PATH_LOG, 
        backtrace=True, diagnose=True, level='DEBUG',
        format='{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
    FILE_LOG_EXISTS = True

def start_log(exits_log: bool = False):
    remove_log()
    add_log_console()
    add_log_file(exits_log)

start_log()
logger.info('Configurando entorno local...')

spark = (
    SparkSession.builder 
    .appName("ETL_Medallion_Emision_SCTR")
    .master("local[*]")
    .config("spark.driver.memory", "18g")
    .config("spark.local.dir", TEMP_DIR.as_posix())
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
    .config("spark.sql.shuffle.partitions", "10")
    .config("spark.sql.session.timeZone", "America/Lima")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .getOrCreate()
)   

spark.sparkContext.setLogLevel("ERROR")
logger.info("Sesión Iniciada. ETL Arq.Medallion lista.")

def validate_path_delta(layer: str, table_name: str, show_message : bool = True) -> bool:
    path = f"{PATH_LAKE}/{layer}/{table_name}"
    is_delta = DeltaTable.isDeltaTable(spark, path)
    if show_message:
        if is_delta:
            logger.info(f"   ✅ La tabla Delta existe en {layer.upper()} / {table_name}.")
        else:
            logger.info(f"   ❌ La tabla Delta NO existe en {layer.upper()} / {table_name}.")
    return is_delta

def save_to_delta(df: DataFrame, layer: str, table_name: str, mode="overwrite"):
    logger.info(f"   ⏳ Preparando escritura en Delta ({table_name})...")
    path = f"{PATH_LAKE}/{layer}/{table_name}"
    # Persistir el DF para no re-calcular todo dos veces
    df.persist()
    try:
        logger.info("   📋 Schema del DataFrame:")
        df.printSchema()

        total_rows = df.count()
        logger.info(f"   📊 Total Registros a procesar: {total_rows:,.0f}")

        logger.info("   ⏳ Escribiendo datos en Delta (Disco)...")
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)
        logger.info(f"   💾 Guardado en {layer.upper()} / {table_name}")
        logger.info(f"   📁 Ruta : {path}")
    except Exception as e:
        logger.error(f"   ❌ Error guardando en Delta ({layer.upper()} / {table_name}): {e}")
        raise e
    
def save_to_parquet(df: DataFrame, layer: str, name_file: str):
    try:
        logger.info("   ⏳ Preparando datos del origen...")
        df.persist()

        total_rows = df.count()
        logger.info(f"   📊 Total Registros a Exportar: {total_rows:,.0f}")

        final_file_path = BASE_PATH / "Consolidados_Ordenados" / name_file
        final_file_path.parent.mkdir(parents=True, exist_ok=True)
        temp_output_folder = BASE_PATH / "Consolidados_Ordenados" / "Temp_Spark_Output"

        if layer == "Gold":
            logger.info("   ⏳ Ordenando datos (Sort)...")
            # df_sorted = df.orderBy(F.col("NUM_DOC").asc())
            df = df.orderBy(
                F.asc_nulls_last("ULT_DIGI_DOC"), 
                F.asc("NUM_DOC")
            )
            # df_sorted = df.orderBy(F.asc("ULT_DIGI_DOC"), F.asc("NUM_DOC"))
            # df_sorted = df.orderBy(["ULT_DIGI_DOC", "NUM_DOC"], ascending=[True, True])

        logger.info("   📦 Exportando archivo único Parquet...")
        (df.coalesce(1)
           .write
           .mode("overwrite")
           .option("compression", "zstd")
           .parquet(temp_output_folder.as_posix())
        )
        
        part_file = None
        for file in temp_output_folder.iterdir():
            if file.suffix == ".parquet" and file.name.startswith("part-"):
                part_file = file
                break
        
        if part_file:
            if final_file_path.exists():
                final_file_path.unlink()
            
            shutil.move(str(part_file), str(final_file_path))
            logger.debug(f"   Archivo renombrado correctamente: {final_file_path.name}")

            shutil.rmtree(temp_output_folder)
        else:
            logger.warning("   No se encontró el archivo part- file generado.")
    except Exception as e:
        logger.error(f"   ❌ Error guardando Parquet ({final_file_path}): {e}")
        return

def ingest_excel_to_bronze(file_path: Path, col_indices: list, col_names: list) -> DataFrame:
    remove_log()
    add_log_console()
    logger.info(f"   Leyendo Archivo: {file_path.name}...")
    add_log_file(True)

    try:        
        excel_reader = fastexcel.read_excel(file_path)
        pdf_list = []
        dtype_map = {idx: "string" for idx in col_indices}

        for sheet_name in excel_reader.sheet_names:
            try:
                sheet = excel_reader.load_sheet_by_name(
                    sheet_name, 
                    use_columns=col_indices, 
                    dtypes=dtype_map
                )
                
                pdf = sheet.to_pandas()
                
                if len(pdf.columns) != len(col_names):
                    print(f"⚠️ Hoja '{sheet_name}' ignorada: Cantidad de columnas no coincide.")
                    continue
                
                pdf.columns = col_names
                
                for col in pdf.columns:
                    if pdf[col].dtype == "object":
                        pdf[col] = pdf[col].str.strip()
                        
                pdf_list.append(pdf)
            except Exception as e:
                logger.warning(f"   ⚠️ Error leyendo hoja '{sheet_name}', se omite. {e}\nArchivo: ./{file_path.parent.name}/{file_path.name}")
                continue

        if not pdf_list:
            return None

        full_pdf = pd.concat(pdf_list, ignore_index=True)
        
        schema = StructType([StructField(c, StringType(), True) for c in col_names])
        df_spark = spark.createDataFrame(full_pdf, schema=schema)

        df_spark = df_spark.withColumn("FECHA_CARGA", F.lit(PERIODO))
        
        return df_spark
    except Exception as e:
        logger.error(f"   ❌ Error crítico ingestado Bronze archivo {file_path.name}: {e}")
        return None
    
def merge_to_delta(spark, df_new: DataFrame, layer: str, table_name: str, unique_keys: list):
    try:
        path = f"{PATH_LAKE}/{layer}/{table_name}"
    
        logger.info(f"   🔄 Iniciando MERGE (Upsert) en {table_name}...")
        
        condition = " AND ".join([f"t.{col} = s.{col}" for col in unique_keys])
        target_table = DeltaTable.forPath(spark, path)
        
        (target_table.alias("t")
        .merge(
            df_new.alias("s"),
            condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
        )
        logger.info(f"   💾 MERGE Guardado en {layer.upper()} / {table_name}")
        last_operation: dict = target_table.history(1).select("operationMetrics").collect()[0][0]
        num_inserted = last_operation.get("numTargetRowsInserted", "0")
        num_updated = last_operation.get("numTargetRowsUpdated", "0")
        logger.info(f"   📈 Merge Reporte: Insertados={num_inserted}, Actualizados={num_updated}")

        total_rows = target_table.toDF().count()
        # total_rows = int(target_table.detail().select("numRecords").collect()[0][0])
        logger.info(f"   📊 Total Registros Guardados (Post Merge): {total_rows:,.0f}")
    except Exception as e:
        logger.error(f"   ❌ Error en Merge Delta ({layer.upper()} / {table_name}): {e}")
        raise e

def transform_expuestos_silver(periodo = PERIODO, get_only_bronze : bool = False) -> DataFrame:
    try:
        logger.info(f"   🔍 Leyendo Bronze Expuestos (Periodo: {periodo})...")
        
        df = spark.read.format("delta").load(f"{PATH_LAKE}/Bronze/Expuestos_Raw") \
                 .filter(F.col("FECHA_CARGA") == F.lit(periodo))
        
        total_rows = df.count()
        if total_rows == 0: 
            logger.warning("   ⚠️ La tabla Bronze Expuestos no contiene registros.")
            raise
        
        if get_only_bronze:
            return df
        
        date_formats = ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MM-yyyy", "yyyy/MM/dd", "yyyyMMdd", "dd/MM/yy"]

        logger.info("   🔄 Transformando Bronze Expuestos...")
        
        def try_parse_dates(col_name):
            return F.coalesce(*[F.to_date(F.substring(F.col(col_name), 1, 10), fmt) for fmt in date_formats])

        #"long" "int"  
        # df_clean = (
        #     df
        #     .withColumns({
        #         # 1. Lógica del último dígito
        #         "ULT_DIGI_DOC": F.try_cast(F.substring(F.col("NUM_DOC"), -1, 1), "int"),
                
        #         # 2. Lógica de Póliza (Decimal -> Int para manejar "100.00")
        #         # Nota: PySpark permite strings de tipo "decimal(10,2)" o "int" directamente
        #         "POLIZA": F.try_cast(F.col("POLIZA"), "decimal(10,2)").cast("int"),
                
        #         # 3. Lógica de Integers directos (Year, Month, Tipo)
        #         "YEAR_MOV": F.try_cast(F.col("YEAR_MOV"), "int"),
        #         "MONTH_MOV": F.try_cast(F.col("MONTH_MOV"), "int"),
        #         "TIPO_DOC_RAW": F.try_cast(F.col("TIPO_DOC"), "int")
        #     })
        # )

        df_clean = (
            df 
            #.withColumn("ULT_DIGI_DOC", F.substring(F.col("NUM_DOC"), -1, 1).cast(IntegerType())) \
            # .withColumn("ULT_DIGI_DOC", F.try_to_number(F.substring(F.col("NUM_DOC"), -1, 1), IntegerType()))
            # .withColumn("NUM_DOC", 
            #     F.regexp_replace(F.col("NUM_DOC"), r"""['"_]""", "")
            # ) 
            .withColumn("NUM_DOC", 
                F.translate(F.col("NUM_DOC"), "'\"_", "") 
            )
            .withColumn("ULT_DIGI_DOC", F.expr("try_cast(substring(NUM_DOC, -1, 1) as INT)"))
            .withColumn("POLIZA", F.col("POLIZA").cast(DecimalType(scale=2)).cast(LongType()))
            .withColumn("YEAR_MOV", F.col("YEAR_MOV").cast(DecimalType(scale=2)).cast(IntegerType())) 
            .withColumn("MONTH_MOV", F.col("MONTH_MOV").cast(DecimalType(scale=2)).cast(IntegerType())) 
            .withColumn("TIPO_DOC_RAW", F.col("TIPO_DOC").cast(DecimalType(scale=2)).cast(IntegerType())) 
            .withColumn("EXPUESTO", 
                F.regexp_replace(
                    F.trim(F.concat_ws(" ", 
                        F.col("P_NOMBRE"), F.col("S_NOMBRE"), 
                        F.col("AP_PATERNO"), F.col("AP_MATERNO")
                    )), 
                    "  ", " "
                )
            ) 
            .withColumn("TIPO_DOC_DESC", 
                F.when(F.col("TIPO_DOC_RAW") == 1, "DNI")
                .when(F.col("TIPO_DOC_RAW") == 2, "CE")
                .when(F.col("TIPO_DOC_RAW") == 5, "PAS")
                .otherwise("OTROS")
            ) 
            .withColumn("NUM_DOC_CLEAN",
                F.when(
                    (F.length(F.col("NUM_DOC")).isin([5, 6, 7])) & 
                    (F.col("NUM_DOC").rlike("^\\d+$")),
                    F.lpad(F.col("NUM_DOC"), 8, '0') # Zfill
                ).otherwise(F.col("NUM_DOC"))
            ) 
            .drop("TIPO_DOC", "NUM_DOC") 
            .withColumnRenamed("TIPO_DOC_DESC", "TIPO_DOC") 
            .withColumnRenamed("NUM_DOC_CLEAN", "NUM_DOC")
        )

        cols_date = ['F_INI_VIGEN_POLIZA','F_FIN_VIGEN_POLIZA','F_INI_COBERT','F_FIN_COBERT']
        for c in cols_date:
            df_clean = df_clean.withColumn(c, try_parse_dates(c))

        df_clean = df_clean.filter(
            F.col("POLIZA").isNotNull() #& F.col("YEAR_MOV").isNotNull() & F.col("MONTH_MOV").isNotNull()
        )

        df_final = df_clean.withColumn("FECHA_CARGA", F.lit(periodo))

        df_final = df_clean.select(*COLS_NAM_EXP_FINAL) \
                    .dropDuplicates(COLS_NAM_EXP_FINAL)
        
        # df_final = df_clean.select(*COLS_NAM_EXP_FINAL, F.current_date().alias("FECHA_REGISTRO")) \
        #                    .dropDuplicates(COLS_NAM_EXP_FINAL)

        return df_final
    except Exception as e:
        logger.error(f"   ❌ Error en Transformación Silver Expuestos: {e}")
        raise e

def transform_contratantes_silver(periodo = PERIODO, get_only_bronze : bool = False) -> DataFrame:
    try:
        logger.info(f"   🔍 Leyendo Bronze Contratantes (Periodo: {periodo})...")

        df = spark.read.format("delta").load(f"{PATH_LAKE}/Bronze/Contratantes_Raw") \
                 .filter(F.col("FECHA_CARGA") == F.lit(periodo))
        
        total_rows = df.count()
        if total_rows == 0: 
            logger.warning("   ⚠️ La tabla Bronze Contratantes no contiene registros.")
            raise
        
        if get_only_bronze:
            return df
        
        logger.info("   🔄 Transformando Bronze Contratantes...")

        df_clean = (
            df 
            .withColumn("NUM_DOC_CONT", 
                F.translate(F.col("NUM_DOC_CONT"), "'\"_", "") 
            )
            .withColumn("POLIZA", F.col("POLIZA").cast(DecimalType(scale=2)).cast(LongType())) \
            .withColumn("YEAR_MOV", F.col("YEAR_MOV").cast(DecimalType(scale=2)).cast(IntegerType())) 
            .withColumn("MONTH_MOV", F.col("MONTH_MOV").cast(DecimalType(scale=2)).cast(IntegerType())) 
            .withColumn("TIPO_DOC_RAW", F.col("TIPO_DOC").cast(DecimalType(scale=2)).cast(IntegerType())) 
            .withColumn("TIPO_DOC_DESC", 
                F.when(F.col("TIPO_DOC_RAW") == 1, "DNI")
                .when(F.col("TIPO_DOC_RAW") == 6, "RUC")
                .otherwise("OTRO")
            ) 
            .withColumn("NUM_DOC_CONT_CLEAN",
                F.when(
                    (F.col("TIPO_DOC_DESC") == "DNI") &
                    (F.length(F.col("NUM_DOC_CONT")).isin([5, 6, 7])) &
                    (F.col("NUM_DOC_CONT").rlike("^\\d+$")),
                    F.lpad(F.col("NUM_DOC_CONT"), 8, '0')
                ).otherwise(F.col("NUM_DOC_CONT"))
            ) 
            .drop("TIPO_DOC", "NUM_DOC_CONT") 
            .withColumnRenamed("TIPO_DOC_DESC", "TIPO_DOC") 
            .withColumnRenamed("NUM_DOC_CONT_CLEAN", "NUM_DOC_CONT")
        )
            
        df_clean = df_clean.filter(
            F.col("POLIZA").isNotNull() & F.col("CONTRATANTE").isNotNull()
        )

        df_final = df_clean.withColumn("FECHA_CARGA", F.lit(periodo))

        df_final = df_clean.select(*COLS_NAM_CONT_FINAL) \
                    .dropDuplicates(COLS_NAM_CONT_FINAL)
        
        # df_final = df_clean.select(*COLS_NAM_CONT_FINAL, F.current_date().alias("FECHA_REGISTRO")) \
        #                    .dropDuplicates(COLS_NAM_CONT_FINAL)

        return df_final
    except Exception as e:
        logger.error(f"   ❌ Error en Transformación Silver Contratantes: {e}")
        raise e

def process_silver(df: DataFrame|None, table_name: str, unique_keys: list):
    status = False
    condition = ", ".join([col for col in unique_keys])

    status = True if df is not None else False
    if not status:
        return

    if validate_path_delta("Silver", table_name):
        df_count = spark.read.format("delta").load(f"{PATH_LAKE}/Silver/{table_name}").count()
        if df_count > 0:
            merge_to_delta(spark, df, "Silver", table_name, unique_keys)
        else:
            save_to_delta(df, "Silver", table_name, mode="overwrite")
    else:
        save_to_delta(df, "Silver", table_name, mode="overwrite")

    if validate_path_delta("Silver", table_name, False):
        logger.info(f"   🧹 Optimizando tabla Silver {table_name}...")
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({condition})")

        logger.info(f"   📋 Analizando tabla Silver {table_name}...")
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR COLUMNS POLIZA")

def process_gold_consolidation() -> DataFrame:
    try:
        logger.info("   🔍 Leyendo Silver Expuestos...")
        df_exp = spark.read.format("delta").load(f"{PATH_LAKE}/Silver/Expuestos")
        
        total_rows = df_exp.count()
        if total_rows == 0: 
            logger.warning("   ⚠️ La tabla Silver Expuestos no contiene registros.")
            # raise Exception(f"La tabla Silver Expuestos no contiene registros.")
            return None

        logger.info("   🔍 Leyendo Silver Contratantes...")
        df_cont = spark.read.format("delta").load(f"{PATH_LAKE}/Silver/Contratantes")

        total_rows = df_cont.count()
        if total_rows == 0: 
            logger.warning("   ⚠️ La tabla Silver Contratantes no contiene registros.")
            return None
        
        df_exp = df_exp.alias("A")
        df_cont = df_cont.alias("B")
        
        # ON A.POLIZA = B.POLIZA AND A.YEAR_MOV = B.YEAR_MOV AND A.MONTH_MOV = B.MONTH_MOV
        join_cond = (
            (F.col("A.POLIZA") == F.col("B.POLIZA")) 
            # &
            # (F.col("A.YEAR_MOV") == F.col("B.YEAR_MOV")) 
            # &
            # (F.col("A.MONTH_MOV") == F.col("B.MONTH_MOV"))
        )
        
        df_joined = df_exp.join(df_cont, join_cond, "left")
        
        cols_select = [
            F.col("A.POLIZA"),
            F.col("A.F_INI_VIGEN_POLIZA"),
            F.col("A.F_FIN_VIGEN_POLIZA"),
            F.col("A.F_INI_COBERT"),
            F.col("A.F_FIN_COBERT"),
            F.col("B.NUM_DOC_CONT"),
            F.col("B.CONTRATANTE"),
            F.col("A.TIPO_DOC"),
            F.col("A.NUM_DOC"),
            F.col("A.ULT_DIGI_DOC"),
            F.col("A.EXPUESTO")
        ]
        
        df_final = df_joined.select(cols_select).distinct()
        
        save_to_delta(df_final, "Gold", "Consolidado_Emision")

        try:
            path_gold = f"{PATH_LAKE}/Gold/Consolidado_Emision"
            logger.info("   🚀 Ejecutando OPTIMIZE & ZORDER...")
            
            spark.sql(f"OPTIMIZE delta.`{path_gold}` ZORDER BY (ULT_DIGI_DOC, NUM_DOC)")
            logger.info("   💯 Tabla Gold / Consolidado_Emision optimizada.")
        except Exception as e:
            logger.warning(f"   ⚠️ No se pudo ejecutar OPTIMIZE: {e}")
    
        return df_final
    except Exception as e:
        logger.error(f"   ❌ Error en Consolidado Gold: {e}")
        raise e

def main():
    RUN_BRONZE = False
    RUN_SILVER = True   
    RUN_GOLD = False

    RUN_EXPUESTOS = True
    RUN_CONTRATANTES = True

    ON_DEMAND = False

    if RUN_BRONZE:
        logger.info("🟠 Iniciando Capa Bronze (Incremental)...")

        if RUN_EXPUESTOS:
            logger.info("   Procesando Expuestos...")
            dfs_exp = []

            for subfolder in PATH_SOURCE_EXP.iterdir():
                if subfolder.is_dir():
                    logger.info(f"   Leyendo archivos Excel de SubCarpeta: {subfolder.parent.name}/{subfolder.name}...")
                    for excel_file in subfolder.glob("*.xlsx"):
                        df = ingest_excel_to_bronze(excel_file, COLS_IDX_EXP, COLS_NAM_EXP)
                        if df: 
                            dfs_exp.append(df)
            
            if dfs_exp:
                full_exp_raw: DataFrame = dfs_exp[0]
                for d in dfs_exp[1:]:
                    full_exp_raw = full_exp_raw.unionAll(d)

                mode = "overwrite" if not validate_path_delta("Bronze", "Expuestos_Raw") else "append"
                save_to_delta(full_exp_raw, "Bronze", "Expuestos_Raw", mode=mode)

        if RUN_CONTRATANTES:
            logger.info("   Procesando Contratantes...")
            dfs_cont = []
            for subfolder in PATH_SOURCE_CONT.iterdir():
                if subfolder.is_dir():
                    for excel_file in subfolder.glob("*.xlsx"):
                        df = ingest_excel_to_bronze(excel_file, COLS_IDX_CONT, COLS_NAM_CONT)
                        if df: 
                            dfs_cont.append(df)
                        
            if dfs_cont:
                full_cont_raw: DataFrame = dfs_cont[0]
                for d in dfs_cont[1:]:
                    full_cont_raw = full_cont_raw.unionAll(d)

                mode = "overwrite" if not validate_path_delta("Bronze", "Contratantes_Raw") else "append"
                save_to_delta(full_cont_raw, "Bronze", "Contratantes_Raw", mode=mode)

    if RUN_SILVER:
        logger.info("⚪ Iniciando Capa Silver (Incremental)...")

        if RUN_EXPUESTOS:
            if ON_DEMAND:
                df_exp_silver = transform_expuestos_silver(datetime.strptime("2025-12-28","%Y-%m-%d").date(), False)
            else:
                df_exp_silver = transform_expuestos_silver()

            keys_exp = ['POLIZA', 'CERTIFICADO', 'NUM_DOC', 'YEAR_MOV', 'MONTH_MOV'] 
            process_silver(df_exp_silver, "Expuestos", keys_exp)

        if RUN_CONTRATANTES:
            if ON_DEMAND:
                df_cont_silver = transform_contratantes_silver(datetime.strptime("2025-12-28","%Y-%m-%d").date(), False)
            else:
                df_cont_silver = transform_contratantes_silver()

            keys_cont = ['POLIZA', 'NUM_DOC_CONT', 'YEAR_MOV', 'MONTH_MOV']
            process_silver(df_cont_silver, "Contratantes", keys_cont)

    if RUN_GOLD:
        logger.info("🟡 Iniciando Capa Gold...")
        df_exp_gold = process_gold_consolidation()
        save_to_parquet(df_exp_gold, "Gold", "Consolidado_PowerBI_PySpark.parquet")

    HORA_FINAL = datetime.now()
    logger.success('✅ Ejecución exitosa: Se procesó la información.')
    difference_time = HORA_FINAL-HORA_INICIAL
    total_seconds = int(difference_time.total_seconds())
    difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60), total_seconds % 60)
    logger.info(f"Tiempo de proceso: {difference_formated}")
    TEMP_DIR.unlink(missing_ok=True)
    sys.exit(0)

if __name__ == '__main__':
    main()