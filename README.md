📊 Stock Data ETL Pipeline with Python, Yahoo Finance & Snowflake
🚀 Overview
This project automates the extraction, transformation, and loading (ETL) of stock market data from Yahoo Finance into Snowflake. It fetches the last 7 days of stock prices, calculates a 7-day moving average, and loads the cleaned data into a Snowflake table.

🛠️ Tech Stack
Python
Yahoo Finance API (yfinance)
Snowflake
Pandas
Dotenv (for environment variables)
Datetime module
📌 Workflow
Extract – Retrieves historical stock data (default: AAPL) from Yahoo Finance.
Transform – Cleans the data and calculates a 7-day moving average.
Load – Inserts the transformed data into a Snowflake table.
📂 Project Structure
bash
Copy
Edit
├── stock_etl.py      # Main ETL script
├── .env              # Environment variables (Snowflake credentials)
├── requirements.txt  # Dependencies
└── README.md         # Project documentation
⚙️ Setup & Usage
1️⃣ Install Dependencies
sh
Copy
Edit
pip install -r requirements.txt
2️⃣ Set Up Environment Variables
Create a .env file with your Snowflake credentials:

ini
Copy
Edit
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
3️⃣ Run the ETL Pipeline
sh
Copy
Edit
python stock_etl.py
✅ The script will extract stock data, transform it, and load it into Snowflake.

🔍 Key Features
✔ Extracts 7-day historical stock data using Yahoo Finance
✔ Calculates 7-day moving average for trend analysis
✔ Automatically creates Snowflake table if not exists
✔ Handles missing data and ensures clean records
✔ Environment variables for secure Snowflake authentication

📜 SQL Table Schema
sql
Copy
Edit
CREATE TABLE IF NOT EXISTS STOCK_DATA (
    date DATE,
    ticker STRING,
    open_price FLOAT,
    close_price FLOAT,
    volume INT,
    "7_day_avg" FLOAT
);
🔹 Example Output
Date	Ticker	Open Price	Close Price	Volume	7-Day Avg
2024-02-27	AAPL	149.50	152.30	1000000	150.40
2024-02-28	AAPL	152.00	153.10	980000	151.20
🔗 Future Enhancements
✅ Add multiple stock tickers support
✅ Automate daily job scheduling with Airflow or Cron
✅ Store data in Parquet/CSV for additional storage
