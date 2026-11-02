import boto3

from config import rw_access_key, rw_secret_key, s3_url, bucket_name

client = boto3.client(
    's3',
    aws_access_key_id=rw_access_key,
    aws_secret_access_key=rw_secret_key,
    endpoint_url=s3_url
)

with open("remote_stream_files.json", "w+") as f:
    for year in [2022, 2023, 2024, 2025, 2026]:
        for month in range(1, 13):
            for day in range(1, 32):
                prefix = "{}-{}-{}".format(year, month, day)
                        
                ob = client.list_objects(Bucket=bucket_name, Prefix="streams/{}".format(prefix))

                if "Contents" in ob:
                    for key in ob['Contents']:
                        f.write("{}\n".format(key["Key"]))