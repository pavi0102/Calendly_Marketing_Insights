"""
Gold: gold_employee_meeting_load
Columns: host_email, week_start_date, meeting_count

Note: grouped by start_time (when the meeting actually happens), not
booking_date (when it was booked) — meeting load is about the host's
calendar, so it should reflect the week the meeting occurs in.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_BOOKINGS_PATH = "s3://calendly-marketing-insights-ps/silver/bookings/"
GOLD_OUTPUT_PATH = "s3://calendly-marketing-insights-ps/gold/employee_meeting_load/"


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("gold-employee-meeting-load")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = get_spark_session()

    silver_df = spark.read.format("delta").load(SILVER_BOOKINGS_PATH)

    gold_df = (
        silver_df.filter(F.col("host_email").isNotNull())
        .withColumn("week_start_date", F.date_trunc("week", F.col("start_time")).cast("date"))
        .groupBy("host_email", "week_start_date")
        .agg(F.count("booking_id").alias("meeting_count"))
    )

    gold_df.write.format("delta").mode("overwrite").save(GOLD_OUTPUT_PATH)
    print(f"Wrote {gold_df.count()} rows to {GOLD_OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()