import json
import boto3


def lambda_handler(event, context):
    body = event["body"]
    payload = json.loads(body)

        #write to S3 landing (batch) + put to Kinesis (speed)
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket="calendly-marketing-insights-ps",
        Key=f"invitee-raw-batch/date={payload['created_at'][:10]}/{payload['payload']['uri'].split('/')[-1]}.json",
        Body=body
    )

    kinesis = boto3.client("kinesis")
    kinesis.put_record(
        StreamName="calendly_invitees",
        Data=body,
        PartitionKey=payload["payload"]["uri"]
    )

    return {"statusCode": 200, "body": "OK"}
   
    
