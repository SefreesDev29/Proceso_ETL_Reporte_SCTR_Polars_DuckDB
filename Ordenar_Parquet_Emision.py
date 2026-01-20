import duckdb
from loguru import logger
from pathlib import Path
from rich.console import Console
from rich import print
import datetime
import sys

HORA_INICIAL = datetime.datetime.now()
PERIODO = str(HORA_INICIAL.year) + str(HORA_INICIAL.month).zfill(2) + str(HORA_INICIAL.day).zfill(2)
PATH_LOG = Path(__file__).resolve().parent / 'Logs' / f'LogApp_{PERIODO}.log'
NOT_EXIST_LOG = True

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
        safe_message = original_message.replace("{", "{{").replace("}", "}}")
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
    
def add_log_file():
    global NOT_EXIST_LOG

    if PATH_LOG.exists() and NOT_EXIST_LOG:
        logger.add(PATH_LOG, 
                backtrace=True, diagnose=True, level='INFO',
                format='\n\n{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
    else:
        logger.add(PATH_LOG, 
            backtrace=True, diagnose=True, level='INFO',
            format='{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
    
    NOT_EXIST_LOG = False

def start_log():
    remove_log()
    add_log_console()
    add_log_file()

base_path = Path(__file__).resolve().parent
input_files_a = base_path / "Consolidados" / "*Expuestos*.parquet"
input_files_b = base_path / "Consolidados" / "*Contratantes*.parquet"
output_folder = base_path / "Consolidados_Ordenados" / "Consolidado_Emision_Ordenado.parquet"

cols_list = ['A.POLIZA','A.F_INI_VIGEN_POLIZA','A.F_FIN_VIGEN_POLIZA',
            'A.F_INI_COBERT','A.F_FIN_COBERT','B.NUM_DOC_CONT','B.CONTRATANTE',
            'A.TIPO_DOC','A.NUM_DOC','A.ULT_DIGI_DOC','A.EXPUESTO']
cols_sql_str = ", ".join(cols_list)

input_str_a = input_files_a.as_posix()
input_str_b = input_files_b.as_posix()
output_str = output_folder.as_posix()

console = Console()
console.rule(f"[grey66]Proceso Iniciado: [bold white]Ordenar Consolidado de Emisión[/bold white][/grey66]")

HORA_INICIAL = datetime.datetime.now()
start_log()
logger.info('Comienzo del Proceso Ordenar Consolidado...')
start_log()

logger.info(f'Abriendo Conexión a DuckDB...')
con = duckdb.connect()

logger.info(f'Configurando límite de Ram...')
con.execute("SET memory_limit='24GB';")

# logger.info(f'Configurando Directorio Temporal...')
# temp_path = base_path / "DuckDB_Temp_Spill"
# temp_path.mkdir(exist_ok=True)
# temp_path_str = temp_path.as_posix()
# con.execute(f"SET temp_directory='{temp_path_str}';")

query = f"""
COPY (
    SELECT DISTINCT 
        {cols_sql_str}
    FROM read_parquet('{input_str_a}') AS A
    LEFT JOIN read_parquet('{input_str_b}') AS B
        ON A.POLIZA = B.POLIZA 
    ORDER BY A.ULT_DIGI_DOC ASC, A.NUM_DOC ASC
) 
TO '{output_str}' 
(
    FORMAT PARQUET, 
    COMPRESSION 'ZSTD', 
    ROW_GROUP_SIZE 120000, 
    OVERWRITE_OR_IGNORE true
);
"""

logger.info('Iniciando optimización con Orden por Póliza...')
con.execute(query)

logger.info('Verificando cantidad de registros generados...')
count_query = f"SELECT count(*) FROM '{output_str}'"
total_rows = con.execute(count_query).fetchone()[0]
logger.info(f'Total de registros: {total_rows}')

HORA_FINAL = datetime.datetime.now()
logger.success('Ejecución exitosa: Consolidado ordenado generado.')
difference_time = HORA_FINAL-HORA_INICIAL
total_seconds = int(difference_time.total_seconds())
difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60), total_seconds % 60)

remove_log()
add_log_file()
logger.info(f'Tiempo de proceso: {difference_formated}')
add_log_console()
print(f'[dark_orange]Tiempo de proceso: {difference_formated}[/dark_orange]')
console.rule(f"[grey66]Proceso Finalizado[/grey66]")
logger.complete()

print("[grey66]Presiona Enter para salir...[/grey66]")
input()
sys.exit(0)