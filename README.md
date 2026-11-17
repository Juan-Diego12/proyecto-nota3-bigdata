# Proyecto Nota 3 - Big Data con pandas y PySpark

Comparación reproducible entre pandas y Apache Spark para procesar los viajes de
taxi amarillo de Nueva York de enero de 2022 a diciembre de 2024.

Ambas versiones ejecutan el mismo proceso: lectura de Parquet, limpieza,
transformaciones, unión con zonas, agregaciones R1-R3 y escritura en Parquet y CSV.
El benchmark compara tiempo, RAM, CPU y escalabilidad.

## Dataset

- Fuente: [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- Periodo: enero de 2022 a diciembre de 2024.
- Archivos: 36 Parquet mensuales y `taxi_zone_lookup.csv`.
- Filas: aproximadamente 119.136.044.
- Tamaño descargado: aproximadamente 1,94 GB.
- Espacio recomendado con muestras y temporales: al menos 3,5 GB.

Los datos no se incluyen en Git. Los scripts del repositorio los descargan y crean
muestras reproducibles del 1 %, 10 % y 50 %. `data/raw` representa el 100 %.

## Estructura

```text
proyecto-nota3-bigdata/
|-- pandas_version/pipeline_pandas.py
|-- spark_version/pipeline_spark.py
|-- scripts/
|   |-- benchmark.py
|   |-- descargar_datos.py
|   |-- generar_graficas.py
|   |-- generar_muestras.py
|   `-- validar_resultados.py
|-- benchmarks/
|-- evidencias/
|-- data/                    # Ignorada por Git
|-- output/                  # Ignorada por Git
|-- requirements.txt
`-- README.md
```

## Requisitos

- Windows 10 u 11 de 64 bits.
- Python 3.11 de 64 bits.
- Eclipse Temurin JDK 17.
- Git.
- Aproximadamente 16 GB de RAM para reproducir el equipo de pruebas.
- Un disco con espacio suficiente para datos y temporales de Spark.

Versiones fijadas en `requirements.txt`:

```text
pyspark==3.5.9
pandas==3.0.6
pyarrow==26.0.1
psutil==7.2.2
matplotlib==3.11.1
```

## 1. Instalación en Windows

### 1.1 Python 3.11

```powershell
winget install --exact --id Python.Python.3.11 --source winget --scope user --accept-package-agreements --accept-source-agreements
```

Cierre y vuelva a abrir PowerShell. Verifique:

```powershell
py -0p
py -3.11 --version
python --version
where.exe python
```

### 1.2 Java 17

En PowerShell como administrador:

```powershell
winget install --exact --id EclipseAdoptium.Temurin.17.JDK --source winget --accept-package-agreements --accept-source-agreements
```

Después de reiniciar PowerShell:

```powershell
java -version
javac -version
where.exe java
```

Si `JAVA_HOME` no se configuró automáticamente:

```powershell
$javaExe = (Get-Command java).Source
$javaBin = Split-Path $javaExe -Parent
$javaHome = Split-Path $javaBin -Parent
[Environment]::SetEnvironmentVariable("JAVA_HOME", $javaHome, "User")
$env:JAVA_HOME = $javaHome
```

### 1.3 Git y clonación

```powershell
winget install --exact --id Git.Git --source winget --accept-package-agreements --accept-source-agreements
```

Después de reiniciar PowerShell:

```powershell
Set-Location ([Environment]::GetFolderPath("MyDocuments"))
git clone https://github.com/Juan-Diego12/proyecto-nota3-bigdata.git
Set-Location .\proyecto-nota3-bigdata
```

### 1.4 Entorno virtual y dependencias

```powershell
py -3.11 -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
& .\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

El prompt debe comenzar con `(.venv)`. En una nueva terminal:

```powershell
Set-Location "RUTA\proyecto-nota3-bigdata"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
& .\.venv\Scripts\Activate.ps1
```

Verifique:

```powershell
python -c "import pandas, pyarrow, psutil, pyspark, matplotlib; print('Dependencias OK')"
```

### 1.5 SPARK_HOME

PySpark instalado con `pip` incluye Spark. Detecte la ruta automáticamente:

```powershell
$sparkHome = (& .\.venv\Scripts\python.exe -c "import inspect, pathlib, pyspark; print(pathlib.Path(inspect.getfile(pyspark)).parent)").Trim()
[Environment]::SetEnvironmentVariable("SPARK_HOME", $sparkHome, "User")
$env:SPARK_HOME = $sparkHome

$sparkBin = Join-Path $sparkHome "bin"
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (($userPath -split ";") -notcontains $sparkBin) {
    [Environment]::SetEnvironmentVariable("Path", "$sparkBin;$userPath", "User")
}
$env:Path = "$sparkBin;$env:Path"
```

```powershell
Test-Path "$env:SPARK_HOME\bin\spark-submit.cmd"
spark-submit --version
```

### 1.6 Hadoop para escritura local

En el equipo probado, Spark podía leer pero fallaba al escribir Parquet sin
`winutils.exe` y `hadoop.dll`.

Apache no distribuye estos binarios para Windows. Para este proyecto académico se
usaron los binarios Hadoop 3.3.6 de
[cdarlint/winutils](https://github.com/cdarlint/winutils). No se recomienda este
procedimiento para producción.

En PowerShell como administrador:

```powershell
$hadoopHome = "C:\hadoop-3.3.6"
New-Item -ItemType Directory -Force "$hadoopHome\bin"

Invoke-WebRequest `
    -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe" `
    -OutFile "$hadoopHome\bin\winutils.exe"

Invoke-WebRequest `
    -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll" `
    -OutFile "$hadoopHome\bin\hadoop.dll"
```

Registre los hashes:

```powershell
Get-FileHash "$hadoopHome\bin\winutils.exe" -Algorithm SHA256
Get-FileHash "$hadoopHome\bin\hadoop.dll" -Algorithm SHA256
```

Configure el entorno:

```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", $hadoopHome, "User")
$hadoopBin = "$hadoopHome\bin"
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (($userPath -split ";") -notcontains $hadoopBin) {
    [Environment]::SetEnvironmentVariable("Path", "$hadoopBin;$userPath", "User")
}
$env:HADOOP_HOME = $hadoopHome
$env:Path = "$hadoopBin;$env:Path"
```

Cierre y vuelva a abrir PowerShell. Active `.venv` y verifique:

```powershell
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').appName('prueba').getOrCreate(); print('HADOOP_NATIVE=',s.sparkContext._jvm.org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded()); s.stop()"
```

El resultado esperado es `HADOOP_NATIVE= True`.

## 2. Descarga y muestras

### Datos dentro del repositorio

```powershell
python scripts\descargar_datos.py
python scripts\generar_muestras.py
```

```text
1 %   -> data\muestras\p01
10 %  -> data\muestras\p10
50 %  -> data\muestras\p50
100 % -> data\raw
zonas -> data\taxi_zone_lookup.csv
```

### Datos en otro disco

Recomendado si el repositorio está dentro de OneDrive:

```powershell
New-Item -ItemType Directory -Force D:\proyecto-nota3-datos

python scripts\descargar_datos.py `
    --destino D:\proyecto-nota3-datos

python scripts\generar_muestras.py `
    --origen D:\proyecto-nota3-datos\raw `
    --destino D:\proyecto-nota3-datos\muestras
```

Verifique 36 archivos en cada carpeta:

```powershell
(Get-ChildItem D:\proyecto-nota3-datos\raw\*.parquet).Count
(Get-ChildItem D:\proyecto-nota3-datos\muestras\p01\*.parquet).Count
(Get-ChildItem D:\proyecto-nota3-datos\muestras\p10\*.parquet).Count
(Get-ChildItem D:\proyecto-nota3-datos\muestras\p50\*.parquet).Count
```

## 3. Ejecución

Los ejemplos usan datos en D. Cambie las rutas si usó `data/`.

### 3.1 pandas

Modo completo en memoria:

```powershell
python pandas_version\pipeline_pandas.py `
    --input D:\proyecto-nota3-datos\muestras\p01 `
    --zones D:\proyecto-nota3-datos\taxi_zone_lookup.csv `
    --output output\pandas_p01 `
    --modo completo
```

Mitigación por archivo mensual:

```powershell
python pandas_version\pipeline_pandas.py `
    --input D:\proyecto-nota3-datos\muestras\p01 `
    --zones D:\proyecto-nota3-datos\taxi_zone_lookup.csv `
    --output output\pandas_archivos_p01 `
    --modo archivos
```

### 3.2 Spark

```powershell
New-Item -ItemType Directory -Force D:\spark-tmp

python spark_version\pipeline_spark.py `
    --input D:\proyecto-nota3-datos\muestras\p01 `
    --zones D:\proyecto-nota3-datos\taxi_zone_lookup.csv `
    --output output\spark_p01 `
    --tmp D:\spark-tmp
```

Argumentos útiles:

- `--api df` o `--api sql`.
- `--memoria 8g`.
- `--particiones 16`.
- `--sin-cache` y `--sin-broadcast` para medir optimizaciones.
- `--explain` para imprimir planes físicos.
- `--pausa` para revisar la Spark UI.

Cada versión genera `r1`, `r2` y `r3` en Parquet, más `r1.csv`, `r2.csv` y
`r3.csv`. Los CSV están redondeados a dos decimales; Parquet conserva la precisión.

## 4. Validación pandas frente a Spark

```powershell
python scripts\validar_resultados.py `
    --pandas output\pandas_p01 `
    --spark output\spark_p01
```

El validador ordena por las claves y usa tolerancia numérica de 0,01. Resultado
obtenido con la muestra del 1 %:

```text
[OK] r1: 255 filas equivalentes
[OK] r2: 168 filas equivalentes
[OK] r3: 20 filas equivalentes
Validacion completa: pandas y Spark producen resultados equivalentes.
```

## 5. Benchmark

El benchmark ejecuta procesos independientes y muestrea cada 0,5 segundos:

- Tiempo total.
- RAM pico del proceso y sus hijos.
- Para Spark, Python más `java.exe`.
- CPU promedio normalizada al porcentaje total del equipo.
- Estado y detalle del error.

Formato:

```csv
herramienta,porcentaje,repeticion,tiempo_s,ram_pico_mb,estado,cpu_promedio,detalle
```

Prueba corta:

```powershell
python scripts\benchmark.py `
    --datos D:\proyecto-nota3-datos `
    --porcentajes 1 `
    --repeticiones 1 `
    --herramientas pandas spark `
    --tmp-spark D:\spark-tmp `
    --resultados output\prueba_resultados.csv
```

Medición oficial en un único equipo:

```powershell
python scripts\benchmark.py `
    --datos D:\proyecto-nota3-datos `
    --porcentajes 1 10 50 100 `
    --repeticiones 3 `
    --herramientas pandas spark `
    --tmp-spark D:\spark-tmp `
    --resultados benchmarks\resultados.csv
```

Se generan 24 filas. El CSV se guarda después de cada ejecución y las claves ya
registradas se omiten al reanudar. Los logs quedan en `benchmarks/logs/`.

## 6. Tablas y gráficas

```powershell
python scripts\generar_graficas.py `
    --resultados benchmarks\resultados.csv `
    --salida benchmarks\graficas `
    --muestras D:\proyecto-nota3-datos\muestras\resumen_muestras.csv
```

Genera `resumen.csv`, `tabla_resumen.md` y gráficas de tiempo, RAM, CPU,
aceleración y optimizaciones. `--ejemplo` usa datos inventados y nunca debe usarse
para el informe final.

## 7. Problemas reales resueltos

### `python` no se reconoce

Use `py -0p` para localizarlo. Si existe, use `py -3.11` o agregue la ruta mostrada
al `PATH` del usuario.

### `ModuleNotFoundError` después de instalar

Active `.venv` y confirme que `(Get-Command python).Source` termine en
`.venv\Scripts\python.exe`.

### `CERTIFICATE_VERIFY_FAILED`

Python no estaba usando correctamente los certificados confiables de Windows:

```powershell
python -m pip install pip-system-certs==5.3
```

Después se reinició PowerShell. No se desactivó la validación SSL.

### `HADOOP_HOME and hadoop.home.dir are unset`

Spark podía calcular, pero fallaba al escribir. Se instaló `winutils.exe` y se
configuró `HADOOP_HOME`.

### `NativeIO$Windows.access0` / `UnsatisfiedLinkError`

`winutils.exe` solo no bastó. Se agregó `hadoop.dll` de la misma versión a
`%HADOOP_HOME%\bin`, se reinició PowerShell y se confirmó `HADOOP_NATIVE=True`.

### `PermissionError: [WinError 5]` en `output`

OneDrive bloqueaba una carpeta Parquet mientras pandas intentaba eliminarla. El
pipeline actual escribe un temporal y reemplaza únicamente el archivo. Los datos y
temporales se mantienen fuera de OneDrive.

### Advertencia sobre `spark.local.dir`

En modo `local[*]`, `--tmp` sigue siendo útil para evitar llenar el disco C. La
advertencia por sí sola no significa que la ejecución haya fallado.

## 8. Evidencias y reproducibilidad

En el equipo oficial se debe registrar:

- Procesador, núcleos, RAM, discos y versión de Windows.
- Versiones de Python, Java, Spark, pandas, PyArrow y psutil.
- `JAVA_HOME`, `SPARK_HOME` y `HADOOP_HOME`.
- Capturas del Administrador de tareas.
- Logs y errores completos.
- Tres repeticiones por herramienta y porcentaje.

Antes de entregar, clone el repositorio en una carpeta nueva y repita la instalación
y ejecución siguiendo solamente este README.

## Equipo

- Daniel Josué Narváez Hincapié.
- Diego Albarracín.
- David Gómez.

Proyecto académico de Introducción al Big Data, Universidad del Quindío, 2026.
