Real-Time Account Aggregation

A real-time data engineering pipeline that consumes account transactions from Apache Kafka, processes them using Apache Spark Structured Streaming, performs 5-minute window-based account aggregation, stores the results as Parquet, and makes the aggregated data available through Apache Hive.

Architecture

Transaction Data
      │
      ▼
 Apache Kafka
 account-transactions
      │
      ▼
Spark Structured Streaming
      │
      ├── Parse Kafka messages
      ├── Validate transactions
      ├── Convert timestamp to event time
      ├── 1-minute watermark
      └── 5-minute window aggregation
      │
      ▼
 Parquet Output
      │
      ▼
    HDFS
      │
      ▼
 Apache Hive
 account_aggregation.account_aggregations

Technologies Used

Scala 2.12.18

Apache Kafka 4.3.1

Apache Spark 3.5.3

Spark Structured Streaming

HDFS

Apache Hive 3.1.2

Apache Parquet

SBT

Java 17

Project Structure

Real-Time-Account-Aggregation/
│
├── src/
│   └── main/
│       └── scala/
│           ├── AccountAggregation.scala
│           ├── KafkaProducer.scala
│           └── Transaction.scala
│
├── build.sbt
├── .gitignore
└── lab03_terminal_output.txt

Generated files such as target/, output/, .bsp/, and project/ are excluded using .gitignore.

Components

1. Transaction Model

Transaction.scala defines the transaction structure:

accountId

transactionType

amount

timestamp

2. Kafka Producer

KafkaProducer.scala publishes account transactions to the Kafka topic:

account-transactions

The account_id is used as the Kafka message key so transactions for the same account can be grouped consistently.

Example transaction:

ACC001,CREDIT,5000.0,2026-09-17 10:00:00

Supported transaction types:

CREDIT

DEBIT

3. Spark Structured Streaming

AccountAggregation.scala reads the Kafka stream and:

Extracts account_id from the Kafka key.

Parses transaction type, amount, and timestamp.

Converts the timestamp into Spark event time.

Validates the incoming records.

Applies a 1-minute watermark.

Groups records by account and a 5-minute time window.

Calculates account-level metrics.

Writes the results as Parquet.

Aggregated Metrics

For each account and 5-minute window:

transaction_count

total_credit

total_debit

average_transaction

maximum_transaction

Sample Aggregation Result

The completed pipeline produced results including:

Account

Window

Count

Credit

Debit

Average

Maximum

ACC002

09:00–09:05

2

3000.0

500.0

1750.0

3000.0

ACC001

09:05–09:10

2

5000.0

1200.0

3100.0

5000.0

ACC002

09:05–09:10

2

3000.0

500.0

1750.0

3000.0

ACC001

09:00–09:05

2

5000.0

1200.0

3100.0

5000.0

HDFS Output

The aggregated Parquet files were stored in:

/user/maha/account_aggregations

Four valid Parquet output files were verified in HDFS.

Hive

Database:

account_aggregation

Table:

account_aggregations

Schema:

account_id              STRING
window_start            TIMESTAMP
window_end              TIMESTAMP
transaction_count       BIGINT
total_credit            DOUBLE
total_debit             DOUBLE
average_transaction     DOUBLE
maximum_transaction     DOUBLE

Hive reads the Parquet files directly from:

/user/maha/account_aggregations

Final verification returned 4 rows.

// Running the Project

1. Start Kafka

Start the Kafka broker using the configured LAB 03 Kafka server configuration.

The Spark application connects to:

172.29.224.1:9092

2. Create/verify the Kafka topic

Topic:

account-transactions

The topic was configured with:

Partitions: 3
Replication Factor: 1

3. Run the Kafka Producer

From the project directory:

sbt "runMain KafkaProducerApp"

Expected output:

Transactions sent successfully!

4. Run the Spark Aggregation

Run:

sbt "runMain AccountAggregation"

The application reads transactions from Kafka and writes the aggregation results to:

output/account_aggregations

5. Verify HDFS

hdfs dfs -ls /user/maha/account_aggregations

6. Verify Hive

USE account_aggregation;

SELECT * FROM account_aggregations;

// Key Concepts Demonstrated:

Kafka producers and topics

Kafka message keys and partitions

Spark Structured Streaming

Event-time processing

Watermarking

Tumbling/window aggregation

Streaming aggregations

Parquet

HDFS

Hive external data location

Scala and SBT

End-to-end real-time data engineering pipeline
