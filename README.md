# Electricity Maps ETL Pipeline

## Overview

This project implements an end-to-end data pipeline using the Electricity Maps API for France (FR). The pipeline follows a layered architecture (Bronze → Silver → Gold) to transform raw API data into structured, analytics-ready datasets.

The outputs of this pipeline are designed to support downstream use cases such as analytics, reporting, and integration with LLM-based systems.

The implementation also includes production-oriented improvements such as retry logic, data validation, and incremental processing.

---

## Pipeline Architecture

The pipeline is organized into three logical layers, each with a clearly defined responsibility.

### Bronze Layer — Raw Data Ingestion

The Bronze layer is responsible for ingesting data from the Electricity Maps API and storing it in its raw form.

Responsibilities:

* Fetch data from API endpoints
* Store responses as JSON without transformation
* Attach metadata:

  * ingestion timestamp
  * source URL
* Partition data by ingestion date (year/month/day)
* Implement retry logic to handle transient API failures

Output:

* Raw JSON files stored in a partitioned directory structure

---

### Silver Layer — Data Processing and Standardization

The Silver layer transforms raw Bronze data into structured datasets using PySpark.

Responsibilities:

* Read raw JSON files from Bronze
* Flatten nested API response structures
* Apply explicit schemas and type casting
* Deduplicate records using business keys
* Generate event-based partition columns (year, month, day)
* Validate data quality before downstream use
* Process only the latest data partition (incremental ingestion)

Validation rules include:

* `zone` must not be null
* `data_datetime` must be valid
* `powerProductionTotal` must be greater than zero

Output:

* Cleaned and structured Parquet datasets

---

### Gold Layer — Business-Level Data Products

The Gold layer produces analytics-ready datasets using Pandas.

Responsibilities:

* Read Silver Parquet datasets
* Perform business-level transformations
* Generate derived metrics for analysis

Datasets produced:

#### Energy Mix (`energy_mix`)

Represents the relative contribution of each energy source to total production.

Columns:

* `zone`
* `data_datetime`
* `solar_pct`
* `wind_pct`
* `hydro_pct`
* `nuclear_pct`
* `coal_pct`
* `gas_pct`

---

#### Import / Export (`import_export`)

Represents electricity flow in and out of France.

Columns:

* `zone`
* `data_datetime`
* `powerImportTotal`
* `powerExportTotal`
* `net_import_export`

---

## Project Structure

```
src/
  bronze.py
  silver.py
  gold.py
  utils.py

docs/
  architecture.md

data/
  bronze/
  silver/
  gold/

requirements.txt
README.md
```

---

## Setup

### 1. Create virtual environment

```
py -3.10 -m venv venv310
venv310\Scripts\activate
```

### 2. Install dependencies

```
pip install -r requirements.txt
```

### 3. Configure API key

Create a `.env` file:

```
ELECTRICITY_MAPS_API_KEY=your_api_key_here
```

---

## Running the Pipeline

Execute each stage sequentially:

### Bronze

```
python src\bronze.py
```

### Silver

```
python src\silver.py
```

### Gold

```
python src\gold.py
```

---

## Data Outputs

### Bronze Layer

```
data/bronze/<stream>/year=YYYY/month=MM/day=DD/*.json
```

---

### Silver Layer

```
data/silver/electricity_flows/parquet/data.parquet
data/silver/electricity_mix/parquet/data.parquet
```

---

### Gold Layer

```
data/gold/energy_mix/data.parquet
data/gold/import_export/data.parquet
```

---

## Additional Improvements

### Error Handling and Retry Logic

API calls in the Bronze layer include retry logic to handle temporary failures. This ensures ingestion reliability without manual intervention.

---

### Data Quality Validation

The Silver layer enforces validation rules to ensure only clean and meaningful data is processed further.

---

### Incremental Processing

The pipeline processes only the latest available Bronze partition, avoiding reprocessing of historical data and improving efficiency.

---

### End-to-End Validation

Each layer was validated by reading output Parquet files and verifying schema correctness and data integrity.

---

## Design Considerations

### Use of PySpark in Silver Layer

PySpark is used for data transformation due to its ability to handle schema enforcement, distributed processing, and scalable data pipelines.

---

### Use of Pandas in Gold Layer

The Gold layer uses Pandas because:

* Transformations are lightweight and column-based
* Data volume is small
* Avoids Hadoop-related issues on Windows

---

### Separation of Layers

Each layer has a clear responsibility:

* Bronze handles ingestion
* Silver handles transformation
* Gold handles business logic

This separation improves maintainability and scalability.

---

### Compatibility with Production Systems

While this implementation runs locally, the design can be extended to:

* Cloud storage (S3 / ADLS)
* Distributed Spark environments (Databricks)
* Orchestrated workflows (Airflow)

---

## Sample Data

Sample output files are included in the `data/` directory to demonstrate pipeline results.

The pipeline can be executed to regenerate fresh data.

---

## Future Improvements

* Workflow orchestration using Airflow
* Cloud storage integration (S3 or ADLS)
* Unit tests for transformation logic
* Data contracts and schema validation
* CI/CD pipeline for automated validation and deployment

---

## Notes

* Spark may log warnings related to temporary file cleanup on Windows
* These do not affect execution or output correctness