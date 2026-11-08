#!/usr/bin/env python3
"""Exercise SQLite through boto3 using only a new, owned disposable container.

Requires boto3. No endpoint argument is accepted: the test creates its own endpoint
and synthetic data, and never connects to an existing Floci instance.
"""

import argparse
import json
import tempfile
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from s3_metadata import docker, memory_sample


def run(image):
    image_id = json.loads(docker("image", "inspect", image).stdout)[0]["Id"]
    report = {"image_id": image_id, "checks": []}
    with tempfile.TemporaryDirectory(prefix="floci-sqlite-sdk-") as temp:
        data = Path(temp)
        data.chmod(0o777)
        container = docker(
            "create",
            "--label",
            "io.floci.sqlite-sdk-test=true",
            "--memory",
            "1g",
            "--cpus",
            "2",
            "--publish",
            "127.0.0.1::4566",
            "--mount",
            f"type=bind,source={data},target=/app/data",
            "--env",
            "FLOCI_STORAGE_MODE=memory",
            "--env",
            "FLOCI_STORAGE_SERVICES_S3_MODE=persistent",
            "--env",
            "FLOCI_STORAGE_SERVICES_S3_OBJECT_INDEX_LAYOUT=sqlite",
            "--env",
            "FLOCI_STORAGE_SERVICES_SQS_MODE=persistent",
            "--env",
            "FLOCI_SERVICES_S3_ENFORCE_AUTH=false",
            "--env",
            "FLOCI_SERVICES_IAM_ENFORCEMENT_ENABLED=false",
            "--env",
            "FLOCI_SERVICES_UI_ENABLED=false",
            image,
        ).stdout.strip()

        def start():
            docker("start", container)
            port = json.loads(docker("inspect", container).stdout)[0][
                "NetworkSettings"
            ]["Ports"]["4566/tcp"][0]["HostPort"]
            endpoint = "http://127.0.0.1:" + port
            for _ in range(120):
                try:
                    with urllib.request.urlopen(
                        endpoint + "/_floci/init", timeout=1
                    ) as response:
                        if json.load(response)["completed"]["ready"]:
                            return endpoint
                except (OSError, ValueError):
                    pass
                time.sleep(0.5)
            raise AssertionError("test container did not become ready")

        def client(service, endpoint):
            return boto3.client(
                service,
                endpoint_url=endpoint,
                region_name="us-west-2",
                aws_access_key_id="test",
                aws_secret_access_key="test",
                config=Config(
                    retries={"max_attempts": 0},
                    read_timeout=10,
                    s3={"addressing_style": "path"},
                ),
            )

        def stop():
            docker("stop", "--time", "120", container, timeout=135)

        try:
            endpoint = start()
            s3 = client("s3", endpoint)
            bucket = "sqlite-sdk-test"
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
            )
            for key in ["a/one", "a/two", "b/three", "literal#v#key", "unicode/😀"]:
                s3.put_object(Bucket=bucket, Key=key, Body=key.encode())
            rows = [
                obj["Key"]
                for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, PaginationConfig={"PageSize": 2}
                )
                for obj in page.get("Contents", [])
            ]
            assert rows == [
                "a/one",
                "a/two",
                "b/three",
                "literal#v#key",
                "unicode/😀",
            ], rows
            groups = s3.list_objects_v2(Bucket=bucket, Delimiter="/", MaxKeys=1)
            assert (
                groups["CommonPrefixes"] == [{"Prefix": "a/"}] and groups["IsTruncated"]
            )
            assert (
                s3.get_object(Bucket=bucket, Key="unicode/😀")["Body"].read()
                == "unicode/😀".encode()
            )
            report["checks"].append(
                "UTF-8 keys, literal version delimiter, object pages and common-prefix seeks"
            )

            s3.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
            )
            first = s3.put_object(Bucket=bucket, Key="versioned", Body=b"first")[
                "VersionId"
            ]
            second = s3.put_object(Bucket=bucket, Key="versioned", Body=b"second")[
                "VersionId"
            ]
            s3.put_object_tagging(
                Bucket=bucket,
                Key="versioned",
                VersionId=first,
                Tagging={"TagSet": [{"Key": "kept", "Value": "yes"}]},
            )
            s3.delete_object(Bucket=bucket, Key="versioned", VersionId=second)
            assert (
                s3.get_object(Bucket=bucket, Key="versioned")["Body"].read() == b"first"
            )
            s3.delete_object(Bucket=bucket, Key="versioned")
            pages = list(
                s3.get_paginator("list_object_versions").paginate(
                    Bucket=bucket, Prefix="versioned", PaginationConfig={"PageSize": 1}
                )
            )
            assert sum(len(page.get("Versions", [])) for page in pages) == 1
            assert sum(len(page.get("DeleteMarkers", [])) for page in pages) == 1
            assert s3.get_object_tagging(
                Bucket=bucket, Key="versioned", VersionId=first
            )["TagSet"] == [{"Key": "kept", "Value": "yes"}]
            report["checks"].append(
                "version promotion, tags, delete markers and version pagination"
            )

            upload = s3.create_multipart_upload(Bucket=bucket, Key="multipart")[
                "UploadId"
            ]
            body = b"a" * (5 * 1024 * 1024)
            parts = []
            for number, content in [(1, body), (2, b"tail")]:
                result = s3.upload_part(
                    Bucket=bucket,
                    Key="multipart",
                    UploadId=upload,
                    PartNumber=number,
                    Body=content,
                )
                parts.append({"PartNumber": number, "ETag": result["ETag"]})
            s3.complete_multipart_upload(
                Bucket=bucket,
                Key="multipart",
                UploadId=upload,
                MultipartUpload={"Parts": parts},
            )
            assert (
                s3.get_object(Bucket=bucket, Key="multipart")["Body"].read()
                == body + b"tail"
            )
            url = s3.generate_presigned_url(
                "put_object", Params={"Bucket": bucket, "Key": "presigned"}
            )
            with urllib.request.urlopen(
                urllib.request.Request(url, data=b"presigned body", method="PUT")
            ) as response:
                assert response.status == 200
            assert (
                s3.get_object(Bucket=bucket, Key="presigned")["Body"].read()
                == b"presigned body"
            )
            report["checks"].append("multipart bodies and presigned PUT")

            sqs = client("sqs", endpoint)
            queue = sqs.create_queue(QueueName="sqlite-events")["QueueUrl"]
            arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])[
                "Attributes"
            ]["QueueArn"]
            s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration={
                    "QueueConfigurations": [
                        {
                            "QueueArn": arn,
                            "Events": ["s3:ObjectCreated:Put"],
                            "Filter": {
                                "Key": {
                                    "FilterRules": [
                                        {"Name": "prefix", "Value": "notify/"}
                                    ]
                                }
                            },
                        }
                    ]
                },
            )
            s3.put_object(Bucket=bucket, Key="notify/object", Body=b"event")
            delivered = False
            for _ in range(10):
                messages = sqs.receive_message(
                    QueueUrl=queue, MaxNumberOfMessages=10, WaitTimeSeconds=1
                ).get("Messages", [])
                for message in messages:
                    event = json.loads(message["Body"])
                    delivered |= any(
                        record["s3"]["object"]["key"] == "notify/object"
                        for record in event.get("Records", [])
                    )
                    sqs.delete_message(
                        QueueUrl=queue, ReceiptHandle=message["ReceiptHandle"]
                    )
                if delivered:
                    break
            assert delivered, "S3 notification was not delivered"
            report["checks"].append("S3 to SQS notification delivery")

            s3.put_object(Bucket=bucket, Key="old-log", Body=b"expired")
            stop()
            endpoint = start()
            s3 = client("s3", endpoint)
            assert (
                s3.get_object(Bucket=bucket, Key="multipart")["Body"].read()
                == body + b"tail"
            )
            assert (
                s3.get_object(Bucket=bucket, Key="versioned", VersionId=first)[
                    "Body"
                ].read()
                == b"first"
            )
            report["checks"].append(
                "clean restart preserves current and historical bodies"
            )

            s3.put_object(Bucket=bucket, Key="crash-recovery", Body=b"committed")
            docker("kill", "--signal", "KILL", container)
            endpoint = start()
            s3 = client("s3", endpoint)
            assert (
                s3.get_object(Bucket=bucket, Key="crash-recovery")["Body"].read()
                == b"committed"
            )
            report["checks"].append(
                "acknowledged metadata survives SIGKILL and SQLite WAL recovery"
            )

            stop()
            snapshot = data / "s3-objects.json"
            metadata = json.loads(snapshot.read_text())
            metadata = {
                key: value
                for key, value in metadata.items()
                if value["key"] != "old-log"
            }
            snapshot.write_text(json.dumps(metadata))
            endpoint = start()
            s3 = client("s3", endpoint)
            assert (
                s3.get_object(Bucket=bucket, Key="crash-recovery")["Body"].read()
                == b"committed"
            )
            try:
                s3.head_object(Bucket=bucket, Key="old-log")
                raise AssertionError("offline cleanup was not imported")
            except ClientError as error:
                assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
            report["checks"].append(
                "offline JSON cleanup is reimported without resurrecting removed metadata"
            )
            report["memory"] = memory_sample(container)
            report["passed"] = True
        except Exception as error:
            report["passed"] = False
            report["error"] = repr(error)
        finally:
            report["logs"] = docker(
                "logs", "--tail", "80", container, check=False
            ).stdout
            try:
                docker("stop", "--time", "120", container, check=False, timeout=135)
            finally:
                docker("rm", "--force", container, check=False)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = run(args.image)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "logs"}, indent=2
        )
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
