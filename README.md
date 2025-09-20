# COVID-19 Healthcare Data Engineering Pipeline

Data pipelines developed during the COVID-19 pandemic to help process and analyze patient data from HealtheIntent systems.

## 🎯 Project Impact

**Designed and maintained production data pipelines that supported COVID-19 response efforts.** These systems worked with real patient encounter data, medication records, and clinical observations from hospital systems during the peak of the pandemic.

## 🏗️ Technical Architecture

### Multi-Database ETL Pipeline

* **PostgreSQL** → **Vertica Data Warehouse** integration
* Processing of **HealtheIntent COVID datasets** (20+ medical data tables)
* Alignment with **OHDSI OMOP** standards for healthcare interoperability
* Automated schema handling and data type inference

### Key Components

| Script                      | Purpose                      | Technical Highlights                            |
| --------------------------- | ---------------------------- | ----------------------------------------------- |
| `load_tables_daily.py`      | Core PostgreSQL ETL Pipeline | Type inference, backup/recovery, error handling |
| `vertica_upload_daily.py`   | Data Warehouse Integration   | Multi-database connectivity, bulk loading       |
| `load_tables_daily_vert.py` | Vertica Processing           | Data warehouse operations, detailed logging     |
| `vertica_upload_quart.py`   | Quarterly Analytics Pipeline | Time-series management, historical archiving    |

## 🚀 Notable Technical Work

### 1. **Data Type Inference** (`load_tables_daily.py`)

```python
def guessType(s):
    # Detects PostgreSQL data types
    # Handles: integers, numeric, boolean, date, timestamp, text
    # Used across 20+ healthcare data tables
```

### 2. **Reliable ETL Pipeline**

* Atomic table operations with rollback options
* Error handling and structured logging
* Table switching designed to avoid downtime

### 3. **Data Warehouse Integration**

* Migration workflows from PostgreSQL → Vertica
* Bulk loading improvements for large datasets
* Schema translation between systems

### 4. **Healthcare Data Standards**

* Implemented **OHDSI OMOP** common data model
* Built in data privacy considerations
* Clinical data validation and quality checks

## 📊 Data Processing Scale

* 20+ healthcare tables processed daily
* Included patient encounters, medications, procedures, conditions
* COVID-19 clinical data handled in near real-time
* Capable of multi-million record batch processing

## 🛠️ Technology Stack

* **Languages**: Python 3.x
* **Databases**: PostgreSQL, Vertica
* **Libraries**: SQLAlchemy, psycopg2, vertica-python, pandas
* **Infrastructure**: Docker, shell scripting
* **Standards**: OHDSI OMOP, HealtheIntent

## 💼 Contribution to Pandemic Response

These pipelines supported:

* Timely analysis of COVID-19 patient outcomes
* Clinical teams with more up-to-date insights
* Research efforts into treatments and responses
* Consistent data quality across hospital systems

## 🔧 Production Features

* Configuration-driven connections
* Centralized logging and monitoring
* Automated backup and recovery steps
* Performance tuning for high-volume loads
* Retry and recovery mechanisms

---

*This project shows hands-on experience with building and running production-ready data pipelines in a high-stakes healthcare environment.*
