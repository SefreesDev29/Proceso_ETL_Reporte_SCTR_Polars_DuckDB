from pathlib import Path
from loguru import logger
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.prompt import IntPrompt
from rich.text import Text
import fastexcel
from contextlib import suppress
import polars as pl
import datetime
import os, sys, shutil, tempfile

# uv run pyinstaller --noconfirm --onefile --strip --icon "Recursos/logo.ico" --hidden-import fastexcel Carga_Reportes_Emision_V3.py 
# uv run pyinstaller --noconfirm --onedir --noupx --strip --icon "Recursos/logo.ico" --hidden-import fastexcel Carga_Reportes_Emision_V3.py 
#--clean --log-level=DEBUG 

HORA_INICIAL, HORA_FINAL = datetime.datetime.now(), datetime.datetime.now()
PERIODO = str(HORA_INICIAL.year) + str(HORA_INICIAL.month).zfill(2) + str(HORA_INICIAL.day).zfill(2)
if getattr(sys, 'frozen', False): 
    PATH_GENERAL = Path(sys.executable).resolve().parent
else:
    PATH_GENERAL = Path(__file__).resolve().parent
PATH_SOURCE_EXP = PATH_GENERAL / 'Reportes_Expuestos' 
PATH_SOURCE_CONT = PATH_GENERAL / 'Reportes_Contratantes' 
PATH_DESTINATION =  PATH_GENERAL / 'Consolidados'
PATH_LOG = PATH_GENERAL / 'Logs' / f'LogApp_{PERIODO}.log'
FILE_LOG_EXISTS = False
REPORT_NAME_EXP = f'Consolidado_Emision_Expuestos_{PERIODO}.parquet' 
REPORT_NAME_CONT = f'Consolidado_Emision_Contratantes_{PERIODO}.parquet' 
REPORT_NAME_FINAL = f'Consolidado_Emision_Final_{PERIODO}.parquet'
FILES_TEMP_REMOVE = []
COLUMNS_INDEX_EXP = [1,2,3,5,6,7,8,9,10,11,12,13,18,19] 
COLUMNS_EXP = ['POLIZA','F_INI_VIGEN_POLIZA','F_FIN_VIGEN_POLIZA',
                'CERTIFICADO','F_INI_COBERT','F_FIN_COBERT',
                'P_NOMBRE','S_NOMBRE','AP_PATERNO','AP_MATERNO','TIPO_DOC','NUM_DOC','YEAR_MOV','MONTH_MOV']
COLUMNS_EXP_FINAL = ['POLIZA','F_INI_VIGEN_POLIZA','F_FIN_VIGEN_POLIZA',
                'CERTIFICADO','F_INI_COBERT','F_FIN_COBERT',
                'TIPO_DOC','NUM_DOC','ULT_DIGI_DOC','EXPUESTO','YEAR_MOV','MONTH_MOV','FECHA_REGISTRO']
COLUMNS_DATE_EXP = ['F_INI_VIGEN_POLIZA','F_FIN_VIGEN_POLIZA','F_INI_COBERT','F_FIN_COBERT']
COLUMNS_INDEX_CONT = [1,2,3,6,8,9]
COLUMNS_CONT = ['TIPO_DOC','NUM_DOC_CONT','CONTRATANTE','POLIZA','YEAR_MOV','MONTH_MOV']
COLUMNS_CONT_FINAL = ['POLIZA','TIPO_DOC','NUM_DOC_CONT','CONTRATANTE','YEAR_MOV','MONTH_MOV','FECHA_REGISTRO']
COLUMNS_INTEGER = ['POLIZA','TIPO_DOC','YEAR_MOV','MONTH_MOV']
FORMATS_DATE = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d",
                "%Y%m%d", "%d/%m/%y", "%d-%m-%y", "%y/%m/%d", "%y-%m-%d"]
ROWS_LIMIT = 10_000_000
NUM_ROWS = 0
ERROR_MSG = None
PROCESS_STATUS = 0

def show_custom_rule(titulo, state = 'Success'):
    ancho_total = console.width
    if state == "Success":
        color_linea = "bold green"
        color_texto = "grey66"
    elif state == "Error":
        color_linea = "red"
        color_texto = "grey66"
    else:
        color_linea = "cyan"
        color_texto = "grey66"

    texto = f" {titulo} "
    largo_texto = len(Text.from_markup(texto).plain)

    largo_linea = max((ancho_total - largo_texto) // 2, 0)
    linea = "─" * largo_linea

    regla = f"[{color_linea}]{linea}[/{color_linea}][{color_texto}]{texto}[/{color_texto}][{color_linea}]{linea}[/{color_linea}]"
    console.print(regla)
    
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

def add_log_file(exits_log: bool):
    global FILE_LOG_EXISTS
    if PATH_LOG.exists() and not exits_log:
        logger.add(PATH_LOG, 
                backtrace=True, diagnose=True, level='INFO',
                format='\n\n{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
        return
    
    logger.add(PATH_LOG, 
        backtrace=True, diagnose=True, level='INFO',
        format='{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
    FILE_LOG_EXISTS = True

def start_log(exits_log: bool = False):
    remove_log()
    add_log_console()
    add_log_file(exits_log)

class MenuPrompt(IntPrompt):
    validate_error_message = "[red]⛔ Error:[/red] Por favor ingrese un número válido."
    
    illegal_choice_message = (
        "[red]⛔ Error:[/red] Por favor seleccione una de las opciones disponibles."
    )

class Process_ETL:
    def __init__(self, process_type: str):
        try:
            self.process = int(process_type)
            if self.process not in [1,2,3]:
                raise Exception()
            self.Process_Start()
        except Exception:
            console.print()
            logger.error('Seleccione una opción válida.')
            print("[grey66]Presiona Enter para salir[/grey66]")
            input()
            sys.exit(1)
    
    def Delete_Temp_Files(self, paths: list[Path]):
        for f in paths:
            f.unlink(missing_ok=True)

    def Read_Excel(self, excel_path: Path, columns_index_original: list[int], columns_names_new: list[str]) -> pl.LazyFrame | None:
        try:
            lf_final = []
            reader = fastexcel.read_excel(excel_path)
            dtypes_map = {idx: "string" for idx in columns_index_original}

            for name in reader.sheet_names:
                try:
                    sheet = reader.load_sheet_by_name(name,use_columns=columns_index_original,dtypes=dtypes_map)
                    q = sheet.to_polars().lazy()
                except Exception:
                    logger.warning(f"No se pudo leer contenido de la hoja '{name}', se omite.\nArchivo Excel : ./{excel_path.parent.name}/{excel_path.name}")
                    continue

                columns_originales = q.collect_schema().names()

                if len(columns_originales) != len(columns_names_new):
                    raise ValueError(f"Cantidad de columnas incorrecta. Permitido: {len(columns_names_new)}")
                
                mapping = dict(zip(columns_originales, columns_names_new))

                q = (
                    q
                    .with_columns([
                        pl.col(col).str.strip_chars() for col in columns_originales
                    ])
                    .rename(mapping)
                    .with_columns(pl.lit(excel_path.name).alias('FILE_SOURCE'))
                )
                    
                lf_final.append(q)
            
            lf: pl.LazyFrame = pl.concat(lf_final)
            n_rows = lf.limit(1).collect(engine='streaming').height
            return lf if n_rows > 0 else None      
        except Exception as e:
            raise Exception(f"{e}\nUbicación Archivo Excel: {excel_path}")

    def Transform_Dataframe_Expuestos(self, lf: pl.LazyFrame, subfolder_path: Path) -> pl.LazyFrame:
        global NUM_ROWS

        q = (
            lf
            .with_columns(pl.col('NUM_DOC').str.replace_all(r"['\"_]", "", literal=False).alias('NUM_DOC'))
            .with_columns(pl.col('NUM_DOC').str.slice(-1).cast(pl.Int8, strict=False).alias('ULT_DIGI_DOC'))
            .with_columns(pl.col(COLUMNS_INTEGER).cast(pl.Float64, strict=False).cast(pl.Int64))
            .with_columns(
                pl.concat_str(
                    [
                        pl.col('P_NOMBRE').fill_null(''),
                        pl.col('S_NOMBRE').fill_null(''),
                        pl.col('AP_PATERNO').fill_null(''),
                        pl.col('AP_MATERNO').fill_null('')
                    ],
                    separator = ' '
                ).str.strip_chars()
                .str.replace_all('  ',' ',literal=True).alias('EXPUESTO')
            )
            .with_columns(
                pl.when(pl.col('TIPO_DOC') == 1)
                .then(pl.lit('DNI'))
                .when(pl.col('TIPO_DOC') == 2)
                .then(pl.lit('CE'))
                .when(pl.col('TIPO_DOC') == 5)
                .then(pl.lit('PAS'))
                .otherwise(pl.lit('OTROS'))
                .alias('TIPO_DOC')
            )
            .with_columns(
                pl.when(
                    (pl.col('NUM_DOC').str.len_chars().is_in([5, 6, 7])) &
                    (pl.col('NUM_DOC').str.contains(r"^\d+$", literal=False))
                )
                .then(pl.col('NUM_DOC').str.zfill(8))
                .otherwise(pl.col('NUM_DOC'))
                .alias('NUM_DOC')
            )
        )
        
        NUM_ROWS = int(q.select(pl.len()).collect(engine='streaming').item())
        logger.info(f"Transformando datos de subcarpeta '{subfolder_path.name}'...")

        errores_poliza = q.filter(pl.col('POLIZA').is_null()).select(['FILE_SOURCE']).unique(['FILE_SOURCE']).collect()

        if errores_poliza.height > 0:
            archivos_afectados = errores_poliza.get_column('FILE_SOURCE').unique().to_list()
            console.print(Panel(f"[bold red]Archivos con datos corruptos en POLIZA:[/bold red]\n{archivos_afectados}"))
            remove_log()
            add_log_file(True)
            logger.info(f'{archivos_afectados}')
            add_log_console()
            raise Exception('PÓLIZAS corruptas (no numéricas).')

        if NUM_ROWS < ROWS_LIMIT:
            def try_parse_date(col_name, formats):
                expressions = [
                    pl.col(col_name).str.slice(0, 10).str.to_date(fmt, strict=False) 
                    for fmt in formats
                ]
                return pl.coalesce(expressions)

            q = q.with_columns([
                try_parse_date(col, FORMATS_DATE).alias(col) 
                for col in COLUMNS_DATE_EXP
            ])
        else:
            q = (
                q
                .with_columns(
                    pl.col(COLUMNS_DATE_EXP)
                    .str.slice(0, 10)
                    .str.to_date(strict=True)
                )
            )

        columns_trans = COLUMNS_EXP.copy()
        COLUMNS_EXP.remove('P_NOMBRE')
        COLUMNS_EXP.remove('S_NOMBRE')
        COLUMNS_EXP.remove('AP_PATERNO')   
        COLUMNS_EXP.remove('AP_MATERNO')
        COLUMNS_EXP.extend(['EXPUESTO','FECHA_REGISTRO'])

        q = (
            q
            .drop_nulls(subset=['POLIZA', 'YEAR_MOV', 'MONTH_MOV'])
            .with_columns(pl.lit(datetime.date.today()).cast(pl.Date).alias('FECHA_REGISTRO'))
            .unique(COLUMNS_EXP)
            .select(COLUMNS_EXP_FINAL)
        )

        COLUMNS_EXP.clear()
        COLUMNS_EXP.extend(columns_trans)

        return q

    def Transform_Dataframe_Contratantes(self, lf: pl.LazyFrame, subfolder_path: Path) -> pl.LazyFrame:
        q = (
            lf
            .with_columns(pl.col('NUM_DOC_CONT').str.replace_all(r"['\"_]", "", literal=False).alias('NUM_DOC_CONT'))
            .with_columns(pl.col(COLUMNS_INTEGER).cast(pl.Float64, strict=False).cast(pl.Int64))
            .with_columns(
                pl.when(pl.col('TIPO_DOC') == 1)
                .then(pl.lit('DNI'))
                .when(pl.col('TIPO_DOC') == 6)
                .then(pl.lit('RUC'))
                .otherwise(pl.lit('OTRO'))
                .alias('TIPO_DOC')
            )
            .with_columns(
                pl.when(
                    (pl.col('TIPO_DOC') == 'DNI') &
                    (pl.col('NUM_DOC_CONT').str.len_chars().is_in([5, 6, 7])) &
                    (pl.col('NUM_DOC_CONT').str.contains(r"^\d+$", literal=False)) 
                )
                .then(pl.col('NUM_DOC_CONT').str.zfill(8))
                .otherwise(pl.col('NUM_DOC_CONT'))
                .alias('NUM_DOC_CONT')
            )
        )
        
        logger.info(f"Transformando datos de subcarpeta '{subfolder_path.name}'...")

        errores_poliza = q.filter(pl.col('POLIZA').is_null()).select(['FILE_SOURCE']).unique(['FILE_SOURCE']).collect()

        if errores_poliza.height > 0:
            archivos_afectados = errores_poliza.get_column('FILE_SOURCE').unique().to_list()
            console.print(Panel(f"[bold red]Archivos con datos corruptos en POLIZA:[/bold red]\n{archivos_afectados}"))
            remove_log()
            add_log_file(True)
            logger.info(f'{archivos_afectados}')
            add_log_console()
            raise Exception('PÓLIZAS corruptas (no numéricas).')

        q = (
            q
            .drop_nulls(subset=['POLIZA', 'CONTRATANTE', 'YEAR_MOV', 'MONTH_MOV']) 
            .with_columns(pl.lit(datetime.date.today()).cast(pl.Date).alias('FECHA_REGISTRO'))
            .unique(COLUMNS_CONT)
            .select(COLUMNS_CONT_FINAL)
        )

        return q

    def Export_Final_Report(self, process_name: str, lf: pl.LazyFrame, report_name: Path):
        global FILES_TEMP_REMOVE

        path_prev = Path(tempfile.gettempdir()) / report_name
        path_report = PATH_DESTINATION / report_name

        q: pl.LazyFrame = lf
        logger.info(f"Total Registros: {q.select(pl.len()).collect(engine='streaming').item()}")
        
        logger.info(q.collect_schema())

        if process_name == 'Contratantes':
            self.lf_dev = (
                q
                .unique(['POLIZA','TIPO_DOC','NUM_DOC_CONT'])
                .filter(pl.struct(['POLIZA']).is_duplicated())
                .sort(['POLIZA'])
            )
            logger.info(f"Total Registros Duplicados: {self.lf_dev.select(pl.len()).collect(engine='streaming').item()}")

        logger.info(f'Verificando si existe consolidado {process_name}...')
        FILES_TEMP_REMOVE.append(path_prev)
        self.Delete_Temp_Files(FILES_TEMP_REMOVE)

        logger.info(f'Generando archivo consolidado {process_name}...')
        q.sink_parquet(path_prev, 
            compression = 'zstd', 
            compression_level = 3, 
            row_group_size = 1 * 1_000_000,
            statistics = True
        )

        logger.info(f'Guardando archivo final {process_name}...')
        if os.path.exists(path_prev):
            with suppress(FileNotFoundError):
                path_report.unlink(missing_ok=True)
                shutil.move(path_prev,path_report)

    def Process_Start(self):
        global HORA_INICIAL, ERROR_MSG, PROCESS_STATUS

        HORA_INICIAL = datetime.datetime.now()

        nombres = {"1": "Cargar Base Expuestos", "2": "Cargar Base Contratantes", "3": "Cargar Ambas Bases (Expuestos/Contratantes)"}
        nombre_proceso = nombres.get(str(self.process).strip(), "Proceso Desconocido")
        console.rule(f"[grey66]Proceso Iniciado: [bold white]{nombre_proceso}[/bold white][/grey66]")
        remove_log()
        if FILE_LOG_EXISTS:
            PATH_LOG.unlink(missing_ok=True)
        add_log_file(False)
        logger.info(f'Comienzo del Proceso {nombre_proceso}...')
        remove_log()
        start_log(True)
        try:
            if self.process == 1 or self.process == 3:
                if not PATH_SOURCE_EXP.exists():
                    raise FileNotFoundError(f"La carpeta principal no existe o tiene un nombre diferente de 'Reportes_Expuestos'.\nUbicación Carpeta Esperada: {PATH_SOURCE_EXP}")
            
            if self.process == 2 or self.process == 3:
                if not PATH_SOURCE_CONT.exists():
                    raise FileNotFoundError(f"La carpeta principal no existe o tiene un nombre diferente de 'Reportes_Contratantes'.\nUbicación Carpeta Esperada: {PATH_SOURCE_CONT}")

            if not PATH_DESTINATION.exists():
                raise FileNotFoundError(f"La carpeta destino no existe..\nUbicación Carpeta Esperada: {PATH_DESTINATION}")            
            
            def processing_excels(excels_files_list: list[Path], columns_index_original: list[int], columns_names_new: list[str]):
                for excel in excels_files_list:
                    lf = self.Read_Excel(excel, columns_index_original, columns_names_new)         
                    if lf is None:
                        raise Exception(f"El archivo Excel no cuenta con información.\nUbicación Archivo Excel: {excel}")
                    yield lf
            
            def processing_subfolders(process_name: str,subfolders_list: list[Path]):
                logger.info(f'Recorriendo contenido de SubCarpetas {process_name}...')
                for folder in subfolders_list:
                    excels = [f for f in folder.iterdir() if f.suffix in ['.xlsx','.xls']]
                    if not excels:
                        logger.warning(f'No se encontraron archivos Excels en subcarpeta, se omite.\nUbicación Subcarpeta : {folder}')
                        continue
                    
                    if process_name == 'Expuestos':
                        q = self.Transform_Dataframe_Expuestos(pl.concat(processing_excels(excels,COLUMNS_INDEX_EXP,COLUMNS_EXP)), folder)
                    else:
                        q = self.Transform_Dataframe_Contratantes(pl.concat(processing_excels(excels,COLUMNS_INDEX_CONT,COLUMNS_CONT)), folder)
                    excels.clear()
                    yield q

            lf_final_list_exp = pl.LazyFrame()
            if self.process == 1 or self.process == 3:
                logger.info('Recorriendo subcarpetas Expuestos...')
                subfolders_list = [c for c in PATH_SOURCE_EXP.iterdir() if c.is_dir()]
                if not subfolders_list:
                    raise Exception(f"No se encontraron subcarpetas en la carpeta principal.\nUbicación Carpeta Core: {PATH_SOURCE_EXP}")
                
                lf_final_list_exp = pl.concat(processing_subfolders('Expuestos',subfolders_list))

                logger.info('Consolidando información Expuestos...')
                self.Export_Final_Report('Expuestos', lf_final_list_exp, REPORT_NAME_EXP)
            
            lf_final_list_cont = pl.LazyFrame()
            if self.process == 2 or self.process == 3:
                logger.info('Recorriendo subcarpetas Contratantes...')
                subfolders_list = [c for c in PATH_SOURCE_CONT.iterdir() if c.is_dir()]
                if not subfolders_list:
                    raise Exception(f"No se encontraron subcarpetas en la carpeta principal.\nUbicación Carpeta Core: {PATH_SOURCE_CONT}")
                
                lf_final_list_cont = pl.concat(processing_subfolders('Contratantes',subfolders_list))

                logger.info('Consolidando información Contratantes...')
                self.Export_Final_Report('Contratantes', lf_final_list_cont, REPORT_NAME_CONT)
            
            lf_final_list_exp.clear()
            lf_final_list_cont.clear()

            ERROR_MSG = None
            PROCESS_STATUS = 0
        except Exception as e:
            self.Delete_Temp_Files(FILES_TEMP_REMOVE)
            ERROR_MSG = str(e)
            PROCESS_STATUS = 1
        finally:
            self.Process_Finalize()

    def Process_Finalize(self):
        global HORA_FINAL

        HORA_FINAL = datetime.datetime.now()
        if ERROR_MSG is None:
            logger.success('Ejecución exitosa: Se cargó la información.')
        else:
            logger.error(f'Proceso Incompleto. Detalle: {ERROR_MSG}')
        difference_time = HORA_FINAL-HORA_INICIAL
        total_seconds = int(difference_time.total_seconds())
        difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60), total_seconds % 60)

        remove_log()
        add_log_file(True)
        logger.info(f'Tiempo de proceso: {difference_formated}')
        add_log_console()
        print(f'[dark_orange]Tiempo de proceso: {difference_formated}[/dark_orange]')

        if ERROR_MSG is None:
            console.rule("[grey66]Proceso Finalizado[/grey66]")
        else:
            show_custom_rule('Proceso Finalizado con Error', state='Error')
        print("[grey66]Presiona Enter para salir[/grey66]")
        input()
        sys.exit(PROCESS_STATUS)

if __name__=='__main__':
    start_log()
    console = Console()
    menu_text = (
        "[bold grey93]\nSeleccione el tipo de Proceso[/bold grey93]\n\n"
        "[cyan]1.[/] Cargar Base Expuestos\n"
        "[cyan]2.[/] Cargar Base Contratantes\n"
        "[cyan]3.[/] Cargar Ambas Bases (Expuestos/Contratantes)\n"
    )
    console.print(Panel.fit(menu_text, title="[bold]Menú de Procesos[/bold]", border_style="grey50"))
    
    process_type = MenuPrompt.ask(
        "[bold white]Escriba el Nro de opción[/bold white]", 
        choices=["1", "2", "3"]
    )

    Process_ETL(process_type)


