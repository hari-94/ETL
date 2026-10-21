# 📊 Stock Data ETL Pipeline with Python, Yahoo Finance & Snowflake  

## 🚀 Overview  
This project **automates the extraction, transformation, and loading (ETL) of stock market data** from **Yahoo Finance** into **Snowflake**. It fetches the **last 7 days of stock prices**, calculates a **7-day moving average**, and loads the cleaned data into a Snowflake table.

## 🛠️ Tech Stack  
- **Python**
- **Yahoo Finance API (`yfinance`)**
- **Snowflake**
- **Pandas**
- **Dotenv (for environment variables)**
- **Datetime module**

---

## 📌 Workflow  
1️⃣ **Extract** – Retrieves historical stock data (default: AAPL) from **Yahoo Finance**.  
2️⃣ **Transform** – Cleans the data and calculates a **7-day moving average**.  
3️⃣ **Load** – Inserts the transformed data into a **Snowflake table**.

---

## 📂 Project Structure  
 - stock_etl.py # Main ETL script 
 - .env.example # Template for .env, which holds the Snowflake credentials and is not committed
 - requirements.txt # Dependencies
 - README.md # Project documentation

🔍 Key Features
✔ Extracts 7-day historical stock data using Yahoo Finance
✔ Calculates 7-day moving average for trend analysis
✔ Automatically creates Snowflake table if not exists
✔ Handles missing data and ensures clean records
✔ Environment variables for secure Snowflake authentication
