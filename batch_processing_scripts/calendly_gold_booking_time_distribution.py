"""
Gold: gold_booking_time_distribution
Columns: day_of_week, booking_date, utm_source, booking_count
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_BOOKINGS_PATH = "s3://calendly-marketing-insights-ps/silver/bookings/"
GOLD_OUTPUT_PATH = "s3://calendly-marketing-insights-ps/gold/booking_time_distribution/"


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("gold-booking-time-distribution")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = get_spark_session()

    silver_df = spark.read.format("delta").load(SILVER_BOOKINGS_PATH)

    gold_df = (
        silver_df.withColumn("booking_date", F.to_date(F.col("booking_date")))
        .withColumn("day_of_week", F.date_format(F.col("booking_date"), "EEEE"))
        .groupBy("day_of_week", "booking_date", "utm_source")
        .agg(F.count("booking_id").alias("booking_count"))
    )

    gold_df.write.format("delta").mode("overwrite").save(GOLD_OUTPUT_PATH)
    print(f"Wrote {gold_df.count()} rows to {GOLD_OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()