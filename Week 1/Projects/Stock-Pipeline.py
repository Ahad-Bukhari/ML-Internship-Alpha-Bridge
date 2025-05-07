import os
import time
import json
import logging
import requests
import pandas as pd
import boto3
import yfinance as yf

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# --- Logging setup ---
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format includes timestamp, log level, and message
    level=logging.INFO  # Set logging level to INFO
)
logger = logging.getLogger(__name__)  # Create logger for the current script/module

# --- Configuration ---
API_KEY = os.getenv("FINNHUB_API_KEY")  # Get API key from environment variables
FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"  # Finnhub URL for stock quotes

KINESIS_STREAM = "data-stream"  # Name of the Kinesis data stream to send data
AWS_REGION = "eu-north-1"  # AWS region for services (e.g., Kinesis, S3)
kinesis = boto3.client("kinesis", region_name=AWS_REGION)  # Kinesis client for interacting with the stream
S3_BUCKET = os.getenv("S3_BUCKET", "my-stock-pipeline")  # S3 bucket to store data (default bucket name)

# --- Airflow Setup ---
# Airflow DAG default arguments, such as owner, start date, retries, etc.
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 1),  # Start date for DAG execution
    "retries": 1,  # Number of retries if a task fails
    "retry_delay": timedelta(minutes=1),  # Delay between retries
    "execution_timeout": timedelta(minutes=30),  # Maximum time for task execution
}

# Define the DAG (Directed Acyclic Graph) for the pipeline, which will be used by Airflow
dag = DAG(
    "stock_data_to_kinesis_with_variation",  # DAG name
    default_args=default_args,  # DAG default arguments
    description="Fetch stock data, validate with GE, send to Kinesis",  # Description of the DAG's purpose
    schedule_interval="*/60 * * * *",  # Schedule the DAG to run every 60 seconds
    catchup=False,  # Don't run missed tasks if the DAG was paused
)

# --- Fetches data from Finnhub ---
def fetch_finnhub_quote(symbol: str) -> dict:
    """Call Finnhub once, then sleep 1s to stay under 60/min rate limit."""
    resp = requests.get(
        FINNHUB_QUOTE_URL,
        params={"symbol": symbol, "token": API_KEY},  # API request to get quote data for a symbol
        timeout=5,  # Timeout in seconds if the request takes too long
    )
    resp.raise_for_status()  # Raise an exception if the response status code is not 200
    data = resp.json()  # Parse the JSON response into a Python dictionary
    time.sleep(1)  # Sleep for 1 second to respect Finnhub's rate limit (max 60 requests/min)
    return data

# --- Fetch all stock symbols from Finnhub for a given exchange (default "US") ---
def fetch_all_finnhub_symbols(exchange="US"):
    url = "https://finnhub.io/api/v1/stock/symbol"  # URL to fetch available symbols for a given exchange
    params = {"exchange": exchange, "token": API_KEY}  # Parameters for the API request
    try:
        resp = requests.get(url, params=params, timeout=10)  # API call to get symbols
        resp.raise_for_status()  # Ensure the response is successful
        symbols = resp.json()  # Parse JSON response
        return [s["symbol"] for s in symbols if s["type"] == "Common Stock"]  # Filter for common stocks
    except Exception as e:
        logger.error(f"❌ Error fetching all Finnhub symbols: {e}")
        return []  # Return an empty list if there's an error

# --- Check if a symbol exists in Finnhub ---
def symbol_exists_in_finnhub(symbol: str) -> bool:
    """Check if a symbol exists in Finnhub by fetching its quote."""
    try:
        quote = fetch_finnhub_quote(symbol)  # Fetch quote for the symbol
        return "c" in quote  # If 'c' (current price) exists, the symbol is valid
    except Exception as e:
        logger.warning(f"❌ Error checking {symbol} in Finnhub: {e}")
        return False  # Return False if there's an error or symbol doesn't exist

# --- Get list of valid symbols, checking both Finnhub and Yahoo Finance ---
def get_symbols() -> list[str]:
    raw = fetch_all_finnhub_symbols()  # Fetch all symbols from Finnhub
    valid: list[str] = []  # List to store valid symbols

    logger.info(f"🔍 Checking {len(raw)} symbols for validity")

    for i, sym in enumerate(raw[:300]):  # Optional limit to check only the first 300 symbols for performance
        sym = sym.upper().strip()  # Convert symbol to uppercase and remove any extra spaces
        if not sym:
            continue  # Skip empty symbols

        if not symbol_exists_in_finnhub(sym):  # Check if symbol is valid in Finnhub
            logger.debug(f"❌ {sym} not in Finnhub")
            continue

        if not symbol_exists_in_yfinance(sym):  # Check if symbol is valid in Yahoo Finance
            logger.debug(f"❌ {sym} not in Yahoo Finance")
            continue

        valid.append(sym)  # Add valid symbols to the list
        logger.info(f"✅ Valid symbol: {sym}")

    if not valid:
        valid = ["AAPL", "GOOG"]  # Default to AAPL and GOOG if no valid symbols found
        logger.warning("⚠ No valid tickers found → defaulting to AAPL, GOOG")

    logger.info(f"✅ Final symbol list: {valid}")
    return valid

# --- Check if a symbol exists in Yahoo Finance ---
def symbol_exists_in_yfinance(symbol: str) -> bool:
    try:
        df = yf.download(symbol, period="1d", interval="1d", progress=False, threads=False)  # Download historical data for 1 day
        return not df.empty  # Return True if the data is not empty
    except Exception:
        return False  # Return False if there's an error fetching data

# --- Get historical price for a symbol on or before a specific date ---
def get_price_on_or_before(symbol, days_ago, retries=3):
    target_date = datetime.today() - timedelta(days=days_ago)  # Calculate the target date
    start = (target_date - timedelta(days=5)).strftime("%Y-%m-%d")  # Start date for the query (5 days before target date)
    end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")  # End date for the query (1 day after target date)

    for attempt in range(retries):  # Retry logic in case of errors
        try:
            df = yf.download(symbol, start=start, end=end, interval="1d", progress=False, threads=False)
            if not df.empty:
                df = df[df.index <= target_date]  # Filter rows before or on the target date
                if not df.empty:
                    price = df["Close"].iloc[-1]  # Get the last close price
                    logger.info(f"📈 {symbol} close price {days_ago}d ago: {round(price,2)}")
                    return round(float(price), 2)  # Return the rounded price
            return None
        except Exception as e:
            logger.warning(f"⚠ Retry {attempt+1}/{retries} for {symbol}: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff between retries
    return None  # Return None if unable to get the price after retries

# --- Calculate variation of current price relative to a historical price ---
def calculate_variation(df, past_price, label):
    if past_price and past_price > 0:
        df[label] = df["current"].apply(
            lambda x: ((x - past_price) / past_price) * 100 if pd.notnull(x) else None  # Percentage change from past price
        )
    else:
        df[label] = None  # Set to None if no valid past price
    return df

# --- Send data to Kinesis stream ---
def send_to_kinesis(df):
    records = df.to_dict(orient="records")  # Convert dataframe to list of records (dicts)
    logger.info(f"📤 Sending {len(records)} records to Kinesis for {df['symbol'].iloc[0]}")  # Log how many records are being sent
    for record in records:
        try:
            resp = kinesis.put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(record).encode("utf-8"),  # Convert record to JSON and encode
                PartitionKey=record["symbol"],  # Use symbol as partition key for Kinesis stream
            )
            logger.info(f"✅ Sent: {resp}")  # Log successful sending
        except Exception as e:
            logger.error(f"❌ Kinesis error for {record['symbol']}: {e}")  # Log error if sending fails

# --- Upload data to S3 ---
def upload_to_s3(df: pd.DataFrame, symbol: str):
    s3 = boto3.client("s3", region_name=AWS_REGION)  # S3 client for uploading data
    today = datetime.utcnow()  # Get the current UTC time
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    
    # Define folder structure for S3 based on current date
    prefix = f"real_time/year={year}/month={month}/day={day}"
    filename = f"{symbol}_{today.strftime('%H%M%S')}.json"  # Filename based on symbol and timestamp
    key = f"{prefix}/{filename}"  # Full S3 key path

    try:
        json_lines = df.to_dict(orient="records")  # Convert dataframe to a list of records
        json_string = "\n".join(json.dumps(line) for line in json_lines)  # Join the records into a JSON string
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json_string.encode("utf-8"),
            ContentType="application/json",  # Specify the content type as JSON
        )
        logger.info(f"✅ Uploaded to S3 at s3://{S3_BUCKET}/{key}")  # Log success
    except Exception as e:
        logger.error(f"❌ Failed to upload {symbol} data to S3: {e}")  # Log failure

# --- Main function to fetch, validate, and send data ---
def fetch_validate_stream():
    logger.info("🚀 Starting fetch_validate_stream")

    try:
        streams = kinesis.list_streams()  # List Kinesis streams
        logger.info(f"🔗 Kinesis streams: {streams['StreamNames']}")  # Log stream names
    except Exception as e:
        logger.error(f"❌ Could not list Kinesis streams: {e}")
        return

    symbols = get_symbols()  # Get the list of valid symbols
    context = gx.get_context(context_root_dir="/opt/airflow/gx")  # Initialize Great Expectations context

    # Register the pandas datasource with Great Expectations
    context.add_datasource(
        name="pandas_datasource",
        class_name="Datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["symbol"],  # Use symbol as batch identifier
            }
        },
    )

    for symbol in symbols:  # Iterate over each symbol
        try:
            logger.info(f"🔍 Processing {symbol}")
            q = fetch_finnhub_quote(symbol)  # Fetch quote from Finnhub
            df = pd.DataFrame(
                [
                    {
                        "symbol": symbol,
                        "current": q["c"],
                        "high": q["h"],
                        "low": q["l"],
                        "open": q["o"],
                        "previous_close": q["pc"],
                    }
                ]
            ).dropna()  # Create DataFrame from the quote data and drop any NaNs

            # Skip if the data is invalid or contains zero values
            if df.empty or not df[["current", "high", "low", "open", "previous_close"]].gt(0).all().all():
                logger.warning(f"⚠ Invalid or empty data for {symbol}; skipping.")
                continue

            df = df.round(2)  # Round numeric columns to 2 decimal places
            df["price_change"] = ((df["current"] - df["previous_close"]) / df["previous_close"]) * 100  # Calculate price change

            # Fetch historical prices and calculate variations
            p30 = get_price_on_or_before(symbol, 30); time.sleep(1)
            p90 = get_price_on_or_before(symbol, 90); time.sleep(1)
            p180 = get_price_on_or_before(symbol, 180)

            df = calculate_variation(df, p30, "variation_1m")
            df = calculate_variation(df, p90, "variation_3m")
            df = calculate_variation(df, p180, "variation_6m")

            # Fill any NaN values in the variation columns with 0
            if df[["variation_1m", "variation_3m", "variation_6m"]].isna().any().any():
                df[["variation_1m", "variation_3m", "variation_6m"]] = df[
                    ["variation_1m", "variation_3m", "variation_6m"]
                ].fillna(0)

            # Create or update the expectation suite for validation
            suite = f"{symbol}_validation_suite"
            context.add_or_update_expectation_suite(suite)

            # Create runtime batch request for validation
            batch_request = RuntimeBatchRequest(
                datasource_name="pandas_datasource",
                data_connector_name="runtime_connector",
                data_asset_name=symbol,
                runtime_parameters={"batch_data": df},
                batch_identifiers={"symbol": symbol},
            )

            # Validate the data using the expectations suite
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite,
            )

            # Define the expectations for validation
            validator.expect_column_values_to_not_be_null("symbol")
            for col in ["current", "high", "low", "open", "previous_close"]:
                validator.expect_column_values_to_be_between(col, min_value=0.01)
            validator.expect_column_values_to_be_of_type("price_change", "float64")
            for var in ["variation_1m", "variation_3m", "variation_6m"]:
                validator.expect_column_values_to_match_regex(var, r"^-?\d+(\.\d+)?$", mostly=0.8)

            # Run validation and send the data if valid
            res = validator.validate()
            logger.info(f"🧪 Validation for {symbol}: success={res.get('success')}")
            if res.get("success"):
                send_to_kinesis(df)  # Send data to Kinesis stream
                upload_to_s3(df, symbol)  # Upload data to S3

            else:
                logger.warning(f"⚠ Validation failed for {symbol}")

        except Exception as e:
            logger.exception(f"❌ Error processing {symbol}: {e}")  # Log errors during processing

# --- Airflow task to run the fetch_validate_stream function ---
task = PythonOperator(
    task_id="fetch_validate_send",  # Task ID for Airflow
    python_callable=fetch_validate_stream,  # Function to be called
    dag=dag,  # Attach this task to the DAG
)
