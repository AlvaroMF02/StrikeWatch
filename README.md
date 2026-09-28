
# ⚡ StrikeWatch

### Análisis y visualización de actividad eléctrica en tiempo real

StrikeWatch es un proyecto de Data Analytics e Ingeniería de Datos que permite capturar, procesar y visualizar eventos de rayos procedentes de la red global de detección de tormentas Blitzortung.

El proyecto integra la adquisición de datos mediante WebSockets, su transformación con Python, el almacenamiento estructurado en MySQL y su posterior análisis mediante un dashboard interactivo desarrollado en Power BI.

El objetivo es construir un flujo ETL completo, desde la recepción de información en tiempo real hasta su transformación en datos útiles para el análisis de la actividad eléctrica.

---

## 🛠️ Tecnologías utilizadas

- **Python:** procesamiento y transformación de datos.
- **aiohttp y asyncio:** conexión WebSocket y recepción asíncrona de eventos.
- **JSON:** interpretación de los mensajes recibidos.
- **reverse_geocoder:** enriquecimiento geográfico mediante coordenadas.
- **MySQL:** almacenamiento relacional de rayos y estaciones detectoras.
- **Power BI:** análisis y visualización interactiva.
- **Jupyter Notebook:** desarrollo y documentación del proceso de captura y transformación.

---

## 🔄 Arquitectura y flujo del proyecto

El sistema sigue un proceso ETL compuesto por las siguientes etapas:

1. Conexión al WebSocket de Blitzortung.
2. Recepción y descompresión de los mensajes mediante un algoritmo basado en LZW.
3. Interpretación de los datos JSON.
4. Limpieza, transformación y enriquecimiento de los registros.
5. Almacenamiento en MySQL.
6. Análisis y visualización de los datos mediante Power BI.

```text
          Blitzortung
              |
              v
       WebSocket (aiohttp)
              |
              v
      Descompresión LZW
              |
              v
         ETL con Python
     Limpieza y geolocalización
              |
              v
            MySQL
      Rayos + Estaciones
              |
              v
           Power BI
      Dashboard interactivo
```

---

## 📂 Estructura del repositorio

```text
StrikeWatch/
│
├── images/              # Capturas del dashboard
├── GuardarRayos.py       # Captura, transformación y almacenamiento en MySQL
├── mysqlXampp.ipynb      # Desarrollo del proceso en Jupyter Notebook
├── strikewatch.pbix      # Dashboard de Power BI
└── README.md             # Documentación del proyecto
```

---

## ⚙️ Procesamiento y transformación de datos

El procesamiento se realiza automáticamente mediante Python.

Entre las operaciones implementadas destacan:

- Descompresión de los mensajes recibidos desde el WebSocket.
- Conversión de timestamps UNIX en nanosegundos a fechas y horas interpretables.
- Conversión horaria a Europe/Madrid.
- Enriquecimiento geográfico mediante latitud y longitud.
- Extracción y estructuración de las variables de cada evento.
- Separación de los rayos y sus estaciones detectoras para su almacenamiento relacional.

### Variables procesadas

| Variable | Descripción |
|---|---|
| time | Timestamp original del evento |
| lat / lon | Coordenadas geográficas |
| alt | Altitud |
| pol | Polaridad |
| mds / mcg | Parámetros adicionales del evento |
| status | Estado del registro |
| region | Región |
| delay | Retraso de señal |
| lonc / latc | Coordenadas adicionales |
| fecha / hora | Fecha y hora transformadas |
| pais | Campo de localización geográfica |
| estaciones | Estaciones detectoras asociadas |

El almacenamiento se realiza mediante dos tablas relacionadas: `rayos` y `estaciones`. Cada evento registrado puede almacenar hasta cinco estaciones detectoras asociadas.

---

## 🚀 Instalación y ejecución

### 1. Clonar el repositorio

```bash
git clone https://github.com/AlvaroMF02/StrikeWatch.git
cd StrikeWatch
```

### 2. Instalar las dependencias

Se recomienda utilizar un entorno virtual de Python.

```bash
python -m venv .venv
```

Activar el entorno virtual en Windows:

```bash
.venv\Scripts\activate
```

Instalar las librerías necesarias:

```bash
pip install aiohttp mysql-connector-python reverse-geocoder
```

### 3. Configurar MySQL

Crear una base de datos llamada `rayos`:

```sql
CREATE DATABASE rayos;
```

Es necesario disponer también de las tablas `rayos` y `estaciones`, con las columnas utilizadas por las consultas INSERT del script.

Configurar los parámetros de conexión a MySQL dentro de `GuardarRayos.py`, utilizando las credenciales del entorno local.

### 4. Ejecutar el programa

```bash
python GuardarRayos.py
```

El programa establecerá la conexión con Blitzortung, procesará los eventos recibidos y realizará su inserción en MySQL.

La versión publicada está configurada para detenerse tras procesar 10 eventos.

### 5. Abrir el dashboard

Abrir `strikewatch.pbix` mediante Power BI Desktop y configurar la conexión con la base de datos correspondiente.

---

## 📊 Resultados y visualización

El proyecto integra la captura, transformación y almacenamiento de eventos eléctricos con una solución de Business Intelligence.

El dashboard desarrollado en Power BI permite:

- Visualizar la actividad eléctrica y su distribución geográfica.
- Explorar los registros mediante filtros y visualizaciones interactivas.
- Analizar las características de los eventos registrados.
- Consultar la información desde diferentes perspectivas mediante gráficos, mapas y tablas.

### Dashboard principal

![Dashboard principal](images/Principal.png)

### Análisis mediante selección de rayos

![Selección por rayo](images/SeleccionPorRayo.png)

### Exploración mediante tablas interactivas

![Selección por tabla](images/SeleccionPorTabla.png)

---

## 📌 Conclusiones y posibles mejoras

StrikeWatch demuestra la integración de diferentes etapas del ciclo de vida del dato: adquisición en tiempo real, descompresión, transformación, almacenamiento relacional y visualización.

El proyecto permite trabajar con datos reales y construir una solución orientada al análisis geográfico de la actividad eléctrica.

La versión publicada constituye un prototipo funcional con una captura limitada a 10 eventos. Entre las posibles mejoras futuras se encuentran:

- Ampliar la captura para permitir una ejecución continua.
- Incorporar una gestión más completa de errores y reconexiones.
- Automatizar la actualización de los datos utilizados por Power BI.
- Externalizar los parámetros de conexión a la base de datos.
- Incorporar un archivo de dependencias y scripts de creación de las tablas SQL.

---

**Fuente de datos:** [Blitzortung](https://www.blitzortung.org/).
