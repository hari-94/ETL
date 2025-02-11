import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def extract_stock_data(ticker="AAPL"):
    """
    Extracts historical stock data from Yahoo Finance.
    """
    stock = yf.Ticker(ticker)
    data = stock.history(period="7d")  # Last 7 days
    data.reset_index(inplace=True)
    data["ticker"] = ticker  # Add a column for stock ticker
    data["extracted_at"] = datetime.now(timezone.utc)  # Timestamp for extraction
    return data

def transform_stock_data(df):
    """
    Cleans data and adds a 7-day moving average column.
    """
    df = df[["Date", "ticker", "Open", "Close", "Volume"]].copy()
    df.rename(columns={"Date": "date", "Open": "open_price", "Close": "close_price"}, inplace=True)
    df["7_day_avg"] = df["close_price"].rolling(window=7).mean()
    df.dropna(inplace=True)  # Remove rows with NaN values
    return df

def load_to_snowflake(df):
    """
    Loads transformed stock data into Snowflake.
    """
    conn = None
    cursor = None
    try:
        # Connect to Snowflake using environment variables
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
        )
        
        cursor = conn.cursor()

        # Try to resume the warehouse (if suspended)
        try:
            cursor.execute(f"ALTER WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')} RESUME;")
        except snowflake.connector.errors.ProgrammingError as e:
            if "cannot be resumed since it is not suspended" in str(e):
                print("Warehouse is already active.")
            else:
                raise e  # Re-raise other errors

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS STOCK_DATA (
                date DATE,
                ticker STRING,
                open_price FLOAT,
                close_price FLOAT,
                volume INT,
                "7_day_avg" FLOAT  -- Enclose in double quotes
            )
        """)

        # Insert data into Snowflake
        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO STOCK_DATA (date, ticker, open_price, close_price, volume, "7_day_avg")
                VALUES ('{row.date}', '{row.ticker}', {row.open_price}, {row.close_price}, {row.Volume}, {row['7_day_avg']})
            """)

        conn.commit()
        print("Data successfully loaded into Snowflake!")

    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        # Ensure cursor and connection are closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # Extract data
    raw_df = extract_stock_data()

    # Transform data
    transformed_df = transform_stock_data(raw_df)
    
    # Print transformed data
    print(transformed_df.head())  # Preview transformed data
    
    # Test Snowflake connection and load data
    load_to_snowflake(transformed_df)