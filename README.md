# OraSync – Data Engineering Pipeline 🚀

## 📌 Overview

OraSync is a Python-based ETL pipeline that simulates data extraction from a source system and loads it into Snowflake using Parquet format.

This project demonstrates real-world data engineering concepts including data ingestion, transformation, and loading into a cloud data warehouse.

---

## 🛠️ Tech Stack

* Python
* Pandas
* Snowflake
* Parquet

---

## 🔄 Pipeline Flow

1. Extract data (simulated source)
2. Convert data into Parquet format
3. Upload files to Snowflake stage
4. Load data using COPY INTO
5. Validate row counts

---

## ✨ Features

* Multi-table pipeline processing
* Dynamic schema creation
* Logging for monitoring
* Data validation checks
* Idempotent loads (TRUNCATE before load)

---

## ▶️ How to Run

```bash
pip install -r requirements.txt
python3 pipeline_final.py
```

---

## 📂 Project Structure

```
orasync/
│
├── pipeline_final.py
├── config.py
├── requirements.txt
├── README.md
│
├── output/
├── logs/
```

---

## 📊 Output

* Data successfully loaded into Snowflake tables
* Logs stored in `/logs`
* Parquet files stored in `/output`

---

## 🎯 Future Enhancements

* Integration with Oracle as source
* Incremental loading using MERGE
* Scheduling with Apache Airflow

---

## 💡 Key Learning

This project demonstrates how to design a scalable and reliable data pipeline using modern data engineering practices.

---

## 👨‍💻 Author

Yash Kakaudiya
# orasync
