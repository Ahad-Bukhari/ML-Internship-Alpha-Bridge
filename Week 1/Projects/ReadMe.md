![Pipeline](https://github.com/user-attachments/assets/ef8c7049-72b0-46b0-90a6-0b6208072599)
Introduction:

Project 2 and 3: Real-time Stock Data Analysis with Automated ETL Pipeline

This Airflow-based pipeline orchestrates periodic fetching, validation, and storage of stock market data. It retrieves real-time stock prices from Yahoo Finance (using the yfinance Python library) and from the Finnhub API. The pipeline validates incoming data with Great Expectations, then sends cleaned records to an AWS Kinesis Data Stream for real-time processing. Finally, validated data is uploaded to an Amazon S3 bucket in a date-partitioned folder structure for efficient querying. Apache Airflow schedules and manages the workflow, Great Expectations ensures data quality, and AWS handles streaming and storage.

> Apache Airflow: An open-source platform to programmatically author, schedule, and monitor workflows airflow.apache.org
> Great Expectations: A Python data validation library that lets you define “expectations” (rules) and automatically check that your datasets meet them bugfree.ai
> Yahoo Finance (yfinance): The yfinance Python package fetches historical and real-time market data from Yahoo Finance pypi.org
> Finnhub API: A free real-time stock market data API for global exchanges finnhubio.github.io
> AWS Kinesis Data Streams: Collects and processes large streams of data records in real time, ideal for pushing stock ticks to downstream consumers docs.aws.amazon.com
> Amazon S3: Serves as the long-term data store, organizing files into partitions (e.g. by date or symbol).

> Features

    Periodic Data Fetching: Pulls stock price data on a schedule from Yahoo Finance and Finnhub. For example, the yfinance library provides a “Pythonic” interface to Yahoo Finance data
    pypi.org
    , and Finnhub offers a free API for real-time stock quotes
    finnhubio.github.io
    .

    Data Validation: Uses Great Expectations to define and enforce data quality rules on the fetched data. Expectations (like “price is positive” or “no missing values”) are automatically checked before further processing
    bugfree.ai
    .

    AWS Streaming: Sends validated records to an AWS Kinesis Data Stream. Kinesis is designed for real-time ingestion of high-volume data (e.g. stock ticks)
    docs.aws.amazon.com
    .

    Partitioned Storage: Uploads cleaned data to Amazon S3. Files are written into a partitioned directory structure (for example, using date-based folders) to enable efficient analytics and retrieval
    lantern.splunk.com
    .

    Containerized Deployment: The project is containerized using Docker Compose for easy setup. This allows running Airflow, its dependencies, and services in isolated Docker containers.

> Prerequisites

Before setting up the project, ensure you have the following:

    Docker & Docker Compose: Install Docker (Desktop or Engine) and Docker Compose. Docker Compose lets you define and run multi-container Docker applications. (Docker Desktop bundles Compose with Docker Engine
    docs.docker.com
    .)

    AWS Credentials: An AWS account with permissions to use Kinesis and S3. Configure an IAM user’s AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (and optionally AWS_DEFAULT_REGION) either via ~/.aws/credentials or environment variables. (The AWS CLI docs note that these env vars override profile settings
    docs.aws.amazon.com
    .) Ensure the IAM user has at least kinesis:PutRecord and s3:PutObject permissions
    docs.aws.amazon.com
    docs.aws.amazon.com
    .

    Finnhub API Key: A valid API key from Finnhub.io. Finnhub offers a free tier (60 calls/minute with a 30 calls/sec cap)
    outsystems.com
    for stock data. Register and copy your FINNHUB_API_KEY.

    Python (for development): Python 3.x installed locally if you plan to run or test parts of the pipeline outside Docker. All other libraries are installed inside the Docker containers.

> Setup Instructions

    Clone the repository.

git clone https://github.com/yourusername/stock-data-pipeline.git
cd stock-data-pipeline

Configure environment variables. Copy the example .env file and populate it with your keys. For example:

cp .env.example .env

Edit .env and set variables:

FINNHUB_API_KEY=your_finnhub_api_key
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1  # e.g. your AWS region
S3_BUCKET=your-s3-bucket-name
KINESIS_STREAM_NAME=your_kinesis_stream_name

These env vars follow the Twelve-Factor App approach of storing config in the environment
12factor.net
. Make sure not to commit secrets to version control.

Build and start the services. Use Docker Compose to build images and launch containers:

docker-compose up -d --build

This creates and starts Airflow (scheduler, webserver, workers) along with dependencies (Postgres, Redis, etc.). On the first run, initialize the Airflow DB and create the admin user by running:

docker-compose run --rm airflow-init

After initialization completes, you should see a message indicating that the admin user has been created
airflow.apache.org
. By default the username is airflow and password is airflow
airflow.apache.org
.

Install Python dependencies (inside the Airflow container). If your DAG uses additional packages (as listed in requirements.txt), install them by connecting to the Airflow webserver container:

docker-compose exec <webserver_service_name> pip install -r requirements.txt

(Replace <webserver_service_name> with the name of your Airflow webserver service, e.g. airflow-webserver or webserver.) Alternatively, you can add dependencies during image build by including requirements.txt in the Dockerfile (see Airflow Docker docs{cursor} for guidance
airflow.apache.org
).

Verify the setup. Ensure containers are running and healthy:

    docker ps

    You should see services for Airflow, Postgres, and Redis up. If you experience issues (e.g. the Airflow webserver keeps restarting), check that Docker has sufficient memory (Airflow suggests at least 4GB allocated
    airflow.apache.org
    ). The first run may take a minute as images are built.

> Running the Airflow DAG

    Airflow Web UI: Open your browser to http://localhost:8080. Log in with the admin credentials (default airflow/airflow
    airflow.apache.org
    ).

    Trigger the DAG: In the UI’s DAGs list, find the stock data pipeline DAG (e.g. stock_data_pipeline). You can either turn on scheduling or manually trigger it by clicking the play/run icon.

    Monitor execution: Check task status in the UI. Logs are available for each task instance, which can help debug failures (e.g. API errors or validation failures).

    Verify output:

        Kinesis Stream: Go to the AWS Management Console > Kinesis > Data Streams. Select your stream and open the Data Viewer tab. You can retrieve recent records directly in the console to confirm data arrival
        docs.aws.amazon.com
        .

        S3 Bucket: In the AWS S3 console, browse to the specified bucket. You should see folders (partitions) corresponding to dates or symbols. Files within these folders contain the validated CSV or JSON stock data. You can download or preview them in the console.

> Environment Variables Reference

    FINNHUB_API_KEY – Your Finnhub API key (free registration at finnhub.io).

    AWS_ACCESS_KEY_ID – AWS access key for an IAM user with Kinesis/S3 permissions
    docs.aws.amazon.com
    .

    AWS_SECRET_ACCESS_KEY – AWS secret key for the IAM user.

    AWS_DEFAULT_REGION – AWS region (e.g. us-east-1), used for Kinesis/S3 endpoints.

    S3_BUCKET – Name of the Amazon S3 bucket where data will be uploaded. Ensure this bucket exists and the IAM user can write to it.

    KINESIS_STREAM_NAME – The name of the AWS Kinesis Data Stream to receive the records. Ensure this stream exists and the IAM user has kinesis:PutRecord permission
    docs.aws.amazon.com
    .

    (Optional) YAHOO_TICKERS – Comma-separated list of stock tickers to fetch (if configured in the DAG).

Include all required variables in your .env file. The Twelve-Factor App recommends storing config like API keys and secrets in environment variables
12factor.net
to keep the codebase clean of secrets.
Troubleshooting

    API rate limits:

        Finnhub: The free Finnhub tier allows up to 60 API calls per minute (and 30 calls/sec)
        outsystems.com
        . Exceeding this limit returns HTTP 429 errors. If you see rate-limit errors in the logs, consider slowing the DAG schedule or upgrading your Finnhub plan.

        Yahoo Finance: While Yahoo Finance does not require an API key, excessive scraping may get blocked or limited. The yfinance library caches data; ensure you’re not requesting the same data too frequently.

    AWS permission errors:

        If the pipeline logs show errors writing to Kinesis/S3, verify your IAM policy. The user needs kinesis:PutRecord (and optionally kinesis:PutRecords) for the stream
        docs.aws.amazon.com
        , and s3:PutObject for the bucket
        docs.aws.amazon.com
        . Attach a policy like AmazonKinesisFullAccess or a custom policy with these actions.

        Check that AWS env vars are correctly set. The AWS CLI docs confirm AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY override profile settings
        docs.aws.amazon.com
        . Make sure no typos in your .env.

    Airflow container issues:

        If services fail to start (webserver/scheduler keeps restarting), ensure Docker has enough memory. The Airflow docs recommend allocating at least 4 GB to Docker
        airflow.apache.org
        .

        If dependencies are missing (ModuleNotFoundError), be sure requirements.txt is installed in the container (see Setup steps) or rebuild the Docker image after updating it.

    Great Expectations failures:

        If a task fails due to a validation error, inspect the output to see which expectation was not met. You may need to adjust the expectation suite in gx/ or the data source. By default, a failed expectation will cause the task (and DAG) to fail. Check the Great Expectations Data Docs or validation logs (in Airflow logs or the great_expectations/uncommitted/validations/ folder) for details.

    Data not appearing in Kinesis/S3:

        Verify the DAG ran successfully (no errors).

        Check that the correct stream and bucket names are set in .env.

        Use the AWS console: for Kinesis, the Data Viewer can show recent records
        docs.aws.amazon.com
        ; for S3, browse the bucket. Confirm files are written under the expected prefixes (e.g. s3://your-bucket/YYYY/MM/DD/filename).

By following these instructions and the references above, we can create a clone of the project in our own environment.
