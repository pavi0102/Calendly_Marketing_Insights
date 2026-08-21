"""
calendly_speed_stream.py

Speed layer job for the Calendly pipeline.
Polls Kinesis directly via boto3 (avoids the Spark Structured Streaming
Kinesis connector, which has no confirmed working build for Spark 4.0.2 /
Scala 2.13 as of this writing), filters to invitee.created events only,
flattens and transforms each event, and writes each polling batch
directly into DynamoDB.

Run via EMR on EC2 as a long-running step (does not terminate).
"""

import json
import time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KINESIS_STREAM_NAME = "calendly_invitees"
KINESIS_REGION = "us-east-1"  # match your stream's AND your DynamoDB table's region
DYNAMODB_TABLE_NAME = "calendly_speed_bookings"
POLL_INTERVAL_SECONDS = 120


def get_spark_session() -> SparkSession:
    return SparkSession.builder.appName("calendly-speed-stream").getOrCreate()


PAYLOAD_SCHEMA = StructType([
    StructField("event", StringType()),  # top-level webhook type: invitee.created, etc.
    StructField("created_at", StringType()),
    StructField("payload", StructType([
        StructField("uri", StringType()),
        StructField("email", StringType()),
        StructField("status", StringType()),
        StructField("scheduled_event", StructType([
            StructField("event_type", StringType()),
            StructField("start_time", StringType()),
            StructField("end_time", StringType()),
            StructField("event_memberships", ArrayType(StructType([
                StructField("user_email", StringType()),
                StructField("user_name", StringType()),
            ]))),
        ])),
        StructField("tracking", StructType([
            StructField("utm_source", StringType()),
        ])),
    ])),
])


def apply_transformations(parsed_df):
    """
    Filters to invitee.created only, flattens the payload, and applies
    the same channel mapping used in Silver so the dashboard sees
    consistent values whether it's reading a batch Gold row or a
    DynamoDB speed row.
    """
    created_only_df = parsed_df.filter(F.col("event") == "invitee.created")

    transformed_df = created_only_df.select(
        F.col("payload.uri").alias("booking_id"),
        F.to_date(F.col("created_at")).cast(StringType()).alias("booking_date"),
        F.col("payload.scheduled_event.event_type").alias("event_type"),
        F.col("payload.scheduled_event.start_time").alias("start_time"),
        F.col("payload.scheduled_event.end_time").alias("end_time"),
        F.col("payload.scheduled_event.event_memberships")[0]["user_email"].alias("host_email"),
        F.col("payload.scheduled_event.event_memberships")[0]["user_name"].alias("host_name"),
        F.col("payload.tracking.utm_source").alias("utm_source"),
    ).withColumn("channel",
                 F.when(F.col("event_type").contains("https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78"), "facebook_paid_ads")
                 .when(F.col("event_type").contains("https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098"), "youtube_paid_ads")
                 .when(F.col("event_type").contains("https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312"), "tiktok_paid_ads")
                 .otherwise("other_event")).filter(F.col("channel").isin(["facebook_paid_ads","youtube_paid_ads","tiktok_paid_ads"]))

    return transformed_df.withColumn(
        "ingested_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss")
    )


def write_batch_to_dynamodb(batch_df, batch_id):
    """
    Converts the Spark DataFrame to plain Python dicts and writes them
    to DynamoDB using boto3's batch_writer, which handles batching/retries
    automatically. Runs on the driver — fine at this data volume (a
    handful of records every 2 minutes).
    """
    rows = batch_df.collect()
    if not rows:
        print(f"Batch {batch_id}: no rows, skipping.")
        return

    dynamodb = boto3.resource("dynamodb", region_name=KINESIS_REGION)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    with table.batch_writer(overwrite_by_pkeys=["booking_id"]) as writer:
        for row in rows:
            item = row.asDict()
            # DynamoDB rejects None values in some SDK paths — drop nulls
            item = {k: v for k, v in item.items() if v is not None}
            writer.put_item(Item=item)

    print(f"Batch {batch_id}: wrote {len(rows)} rows to DynamoDB.")


def poll_kinesis_and_write(spark: SparkSession):
    """
    Polls Kinesis directly via boto3 instead of Spark's Structured
    Streaming Kinesis connector (no confirmed working build exists yet
    for Spark 4.0.2 / Scala 2.13 — see ClassNotFoundException:
    kinesis.DefaultSource). Runs forever, one poll every
    POLL_INTERVAL_SECONDS.

    Note: no checkpointing here — on a job restart, this resumes from
    LATEST (whatever's newest at restart time). Any records that arrived
    during the restart gap are missed rather than replayed. Acceptable
    given invitee.created-only scope and the live/disposable nature of
    the speed layer.
    """
    kinesis_client = boto3.client("kinesis", region_name=KINESIS_REGION)

    shard_id = kinesis_client.list_shards(StreamName=KINESIS_STREAM_NAME)["Shards"][0]["ShardId"]
    shard_iterator = kinesis_client.get_shard_iterator(
        StreamName=KINESIS_STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="LATEST",
    )["ShardIterator"]

    batch_id = 0
    print(f"Starting Kinesis poll loop on stream '{KINESIS_STREAM_NAME}', "
          f"every {POLL_INTERVAL_SECONDS}s.")

    while True:
        response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)
        records = response["Records"]

        if records:
            events = [json.loads(r["Data"]) for r in records]
            batch_df = spark.createDataFrame(events, schema=PAYLOAD_SCHEMA)
            transformed_df = apply_transformations(batch_df)
            write_batch_to_dynamodb(transformed_df, batch_id)
        else:
            print(f"Batch {batch_id}: no new records.")

        shard_iterator = response["NextShardIterator"]
        batch_id += 1
        time.sleep(POLL_INTERVAL_SECONDS)


def main():
    spark = get_spark_session()
    poll_kinesis_and_write(spark)


if __name__ == "__main__":
    main()