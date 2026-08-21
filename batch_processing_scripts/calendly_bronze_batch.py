"""
calendly_bronze_batch.py

Bronze layer job for the Calendly batch pipeline.
Reads raw webhook JSON from S3 landing, applies a stable schema
(no dedup, no cleaning, no business logic), and appends the result
as a Delta Lake table on S3.

Run via EMR Serverless (or EMR on EC2) as a batch job.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, BooleanType
)

# ---------------------------------------------------------------------------
# Config — adjust bucket/paths for your environment
# ---------------------------------------------------------------------------
RAW_LANDING_PATH = "s3://calendly-marketing-insights-ps/invitee-raw-batch/"
BRONZE_OUTPUT_PATH = "s3://calendly-marketing-insights-ps/bronze/"


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("calendly-bronze-batch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_raw_events(spark: SparkSession):
    """
    Reads every JSON file under the raw landing prefix.
    Each file is one Calendly webhook payload, written by the ingestion Lambda.
    """
    return spark.read.option("multiline", "true").json(RAW_LANDING_PATH)


def apply_bronze_schema(raw_df):
    """
    Flattens the nested Calendly webhook payload into a stable, typed
    structure. This is schema enforcement only — no dedup, no filtering,
    no cleaning. Silver is where that logic belongs.
    """
    payload_fields = raw_df.schema["payload"].dataType.fieldNames()
    bronze_df = raw_df.select(
        # top-level event metadata        
        F.col("created_at").cast(TimestampType()).alias("booking_date"),

        # invitee identity
        F.col("payload.uri").alias("booking_id"),
        F.col("payload.email").alias("invitee_email"),
        F.col("payload.first_name").alias("invitee_first_name"),
        F.col("payload.last_name").alias("invitee_last_name"),
        F.col("payload.status").alias("invitee_status"),
        F.col("payload.timezone").alias("invitee_timezone"),

        # scheduling details
        F.col("payload.scheduled_event.event_type").alias("event_type"),
        F.col("payload.scheduled_event.uri").alias("event_uri"),
        F.col("payload.scheduled_event.name").alias("event_name"),
        F.col("payload.scheduled_event.start_time").cast(TimestampType()).alias("start_time"),
        F.col("payload.scheduled_event.end_time").cast(TimestampType()).alias("end_time"),
        F.col("payload.scheduled_event.status").alias("event_status"),

        # host / employee attribution — used later for meeting-load metrics
        F.col("payload.scheduled_event.event_memberships")[0]["user_email"].alias("host_email"),
        F.col("payload.scheduled_event.event_memberships")[0]["user_name"].alias("host_name"),

        # UTM / channel attribution
        F.col("payload.tracking.utm_source").alias("utm_source"),
        F.col("payload.tracking.utm_medium").alias("utm_medium"),
        F.col("payload.tracking.utm_campaign").alias("utm_campaign"),
        F.col("payload.tracking.utm_content").alias("utm_content"),
        F.col("payload.tracking.utm_term").alias("utm_term"),

        # rescheduling flag
        
        F.col("payload.rescheduled").cast(BooleanType()).alias("was_rescheduled"),

        # raw questions/answers kept as-is — Silver decides if/how to flatten
        F.col("payload.questions_and_answers").alias("questions_and_answers"),
    )

    # lineage + partition columns
    bronze_df = (
        bronze_df.withColumn("ingest_date", F.current_date())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    return bronze_df


def write_bronze(bronze_df):
    (
        bronze_df.write.format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .option("mergeSchema", "true")  # tolerate new fields Calendly adds later
        .save(BRONZE_OUTPUT_PATH)
    )


def main():
    spark = get_spark_session()

    raw_df = read_raw_events(spark)
    row_count = raw_df.count()
    print(f"Read {row_count} raw events from {RAW_LANDING_PATH}")

    if row_count == 0:
        print("No new raw events found — nothing to write. Exiting.")
        spark.stop()
        return

    bronze_df = apply_bronze_schema(raw_df)
    write_bronze(bronze_df)

    print(f"Wrote {bronze_df.count()} rows to Bronze at {BRONZE_OUTPUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()