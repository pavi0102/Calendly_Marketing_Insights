"""
calendly_silver_batch.py

Silver layer job for the Calendly batch pipeline.
Reads bronze delta from S3 landing, performs deduplication, cleaning, and business logic, and appends the result
as a Delta Lake table on S3.

Run via EMR Serverless (or EMR on EC2) as a batch job.
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession 
from delta.tables import DeltaTable 
from datetime import date, timedelta
import urllib.request
import json  
from urllib.error import HTTPError

# ---------------------------------------------------------------------------
# Config — adjust bucket/paths for your environment
# ---------------------------------------------------------------------------
BRONZE_INPUT_PATH = "s3://calendly-marketing-insights-ps/bronze/"
SILVER_BOOKINGS_PATH = "s3://calendly-marketing-insights-ps/silver/bookings/"
SILVER_SPENDS_PATH = "s3://calendly-marketing-insights-ps/silver/spends/"

PREVIOUS_DATE = (date.today() - timedelta(days=1)).isoformat()



def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("calendly-silver-batch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

def read_bronze_bookings(spark: SparkSession):
    """
    Reads the bronze delta table from S3.
    """
    return spark.read.format("delta").load(BRONZE_INPUT_PATH)

def read_spend_data(spark: SparkSession, previous_date: str):
    """
    Fetches the marketing spend JSON from a public S3-hosted URL via plain
    HTTP. A browser-style User-Agent is required — this bucket 403s on
    Python's default urllib User-Agent. A missing file (HTTP error of any
    kind) returns an empty DataFrame instead of raising, so a spend-fetch
    problem never blocks the bookings merge in main().
    """
    url = f"https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data/spend_data_{previous_date}.json"

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"}
    )

    try:
        with urllib.request.urlopen(req) as response:
            raw_bytes = response.read()
    except HTTPError as e:
        print(f"No spend data available for {previous_date} (HTTP {e.code}) — skipping spend merge.")
        return spark.createDataFrame([], schema="channel STRING, date STRING, spend DOUBLE")

    parsed = json.loads(raw_bytes)
    if isinstance(parsed, dict):
        parsed = [parsed]  # handle a single-object file, not just a list

    json_strings = [json.dumps(record) for record in parsed]
    rdd = spark.sparkContext.parallelize(json_strings)
    return spark.read.json(rdd)

def clean_deduplicate_bookings(bookings_df):
    """
    Deduplicates the bookings dataframe based on booking_id.
    """
    cleaned_df = bookings_df.filter(F.col("invitee_email").isNotNull() & F.col("booking_id").isNotNull()).dropDuplicates(["booking_id"])
    silver_booking_df = cleaned_df.withColumn("channel",
                                              F.when(F.col("event_type").contains("https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78"), "facebook_paid_ads")
                                              .when(F.col("event_type").contains("https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098"), "youtube_paid_ads")
                                              .when(F.col("event_type").contains("https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312"), "tiktok_paid_ads")
                                              .otherwise("other_event"))
    silver_booking_df = silver_booking_df.filter(F.col("channel").isin(["facebook_paid_ads","youtube_paid_ads","tiktok_paid_ads"]))
    silver_booking_df = silver_booking_df.select("booking_id", "booking_date", "channel", "start_time","end_time","host_email","host_name", "utm_source")
    return silver_booking_df

def merge_into_silver_bookings(spark: SparkSession, silver_df):
    """
    Upserts on booking_id — a booking and any later event for the same
    booking_id collapse to a single row, since clean_deduplicate_bookings
    already deduped upstream on booking_id alone.
    """
    if not DeltaTable.isDeltaTable(spark, SILVER_BOOKINGS_PATH):
        silver_df.write.format("delta").mode("overwrite").save(SILVER_BOOKINGS_PATH)
        return

    target = DeltaTable.forPath(spark, SILVER_BOOKINGS_PATH)
    (
        target.alias("t")
        .merge(
            silver_df.alias("s"),
            "t.booking_id = s.booking_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def clean_spend_data(spend_df):
    """
    Cleans the spend dataframe.
    """
    cleaned_spend_df = spend_df.filter(F.col("channel").isNotNull() & F.col("spend").isNotNull()).dropDuplicates(["channel", "date"]).withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
    return cleaned_spend_df

def merge_into_silver_spend(spark: SparkSession, cleaned_spend_df):
    if not DeltaTable.isDeltaTable(spark, SILVER_SPENDS_PATH):
        cleaned_spend_df.write.format("delta").mode("overwrite").save(SILVER_SPENDS_PATH)
        return

    target = DeltaTable.forPath(spark, SILVER_SPENDS_PATH)
    (
        target.alias("t")
        .merge(cleaned_spend_df.alias("s"), "t.date = s.date AND t.channel = s.channel")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    spark = get_spark_session()
    bookings_df = read_bronze_bookings(spark)

    # bookings processed and merged first, independent of spend — a
    # spend-fetch problem should never block bookings from landing in Silver
    booking_count = bookings_df.count()
    if booking_count > 0:
        print(f"Read {booking_count} bookings from bronze layer.")
        silver_booking_df = clean_deduplicate_bookings(bookings_df)
        merge_into_silver_bookings(spark, silver_booking_df)
        print(f"Merged {silver_booking_df.count()} rows into Silver bookings")
    else:
        print("No bookings found in bronze layer.")

    spend_df = read_spend_data(spark, PREVIOUS_DATE)
    spend_count = spend_df.count()
    if spend_count > 0:
        print(f"Read {spend_count} spend records from S3.")
        cleaned_spend_df = clean_spend_data(spend_df)
        merge_into_silver_spend(spark, cleaned_spend_df)
        print(f"Merged {cleaned_spend_df.count()} rows into Silver marketing spend")
    else:
        print("No spend records found — skipping spend merge.")

    spark.stop()

if __name__ == "__main__":
    main()