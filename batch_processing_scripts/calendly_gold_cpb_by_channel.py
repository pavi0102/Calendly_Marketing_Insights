"""
Gold: gold_cpb_by_channel
Columns: channel, total_bookings, total_spend, cost_per_booking
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_BOOKINGS_PATH = "s3://calendly-marketing-insights-ps/silver/bookings/"
SILVER_SPEND_PATH = "s3://calendly-marketing-insights-ps/silver/spends/"
GOLD_OUTPUT_PATH = "s3://calendly-marketing-insights-ps/gold/cpb_by_channel/"


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("gold-cpb-by-channel")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = get_spark_session()

    bookings_df = spark.read.format("delta").load(SILVER_BOOKINGS_PATH)
    spend_df = spark.read.format("delta").load(SILVER_SPEND_PATH)

    bookings_by_channel = bookings_df.groupBy("channel").agg(
        F.count("booking_id").alias("total_bookings")
    )
    spend_by_channel = spend_df.groupBy("channel").agg(
        F.sum("spend").alias("total_spend")
    )

    gold_df = (
        bookings_by_channel.join(spend_by_channel, on="channel", how="left")
        .fillna(0, subset=["total_spend"])
        .withColumn(
            "cost_per_booking",
            F.when(F.col("total_bookings") > 0, F.col("total_spend") / F.col("total_bookings")).otherwise(0.0),
        )
        .select("channel", "total_bookings", "total_spend", "cost_per_booking")
    )

    gold_df.write.format("delta").mode("overwrite").save(GOLD_OUTPUT_PATH)
    print(f"Wrote {gold_df.count()} rows to {GOLD_OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()