import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AccountAggregation {

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("Real-Time Account Aggregation")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Read transactions from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.29.224.1:9092")
      .option("subscribe", "account-transactions")
      .option("startingOffsets", "earliest")
      .load()

    // Convert Kafka key and value from binary to String
    val transactions = kafkaDF
      .selectExpr(
        "CAST(key AS STRING) AS account_id",
        "CAST(value AS STRING) AS transaction_data"
      )

    // Parse CSV transaction data
    val parsed = transactions
      .select(
        col("account_id"),

        split(col("transaction_data"), ",")
          .getItem(1)
          .alias("transaction_type"),

        split(col("transaction_data"), ",")
          .getItem(2)
          .cast("double")
          .alias("amount"),

        split(col("transaction_data"), ",")
          .getItem(3)
          .alias("timestamp")
      )

    // Convert timestamp string into Spark timestamp
    val withTimestamp = parsed
      .withColumn(
        "event_time",
        to_timestamp(
          col("timestamp"),
          "yyyy-MM-dd HH:mm:ss"
        )
      )

    // Validate transactions
    val validated = withTimestamp
      .filter(
        col("account_id").isNotNull &&
        length(trim(col("account_id"))) > 0
      )
      .filter(
        col("transaction_type").isin("CREDIT", "DEBIT")
      )
      .filter(
        col("amount").isNotNull &&
        col("amount") > 0
      )
      .filter(
        col("event_time").isNotNull
      )

    // 5-minute window aggregation
    val result = validated
 .withWatermark("event_time", "1 minute")
      .groupBy(
        col("account_id"),
        window(
          col("event_time"),
          "5 minutes"
        )
      )
      .agg(

        // 1. Transaction count
        count("*").alias("transaction_count"),

        // 2. Total credit
        sum(
          when(
            col("transaction_type") === "CREDIT",
            col("amount")
          ).otherwise(0.0)
        ).alias("total_credit"),

        // 3. Total debit
        sum(
          when(
            col("transaction_type") === "DEBIT",
            col("amount")
          ).otherwise(0.0)
        ).alias("total_debit"),

        // 4. Average transaction
        avg("amount").alias("average_transaction"),

        // 5. Maximum transaction
        max("amount").alias("maximum_transaction")
      )
      .select(
        col("account_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("transaction_count"),
        col("total_credit"),
        col("total_debit"),
        col("average_transaction"),
        col("maximum_transaction")
      )

    // Write aggregation results to console
    val query = result.writeStream
  .outputMode("append")
  .format("parquet")
  .option("path", "output/account_aggregations")
  .option("checkpointLocation", "output/checkpoint")
  .start()
    // Keep streaming application running
    query.awaitTermination()
  }
}
