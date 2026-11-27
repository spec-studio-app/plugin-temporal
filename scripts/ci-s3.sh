#!/bin/sh
# CI: start an S3-compatible server (SeaweedFS) on 127.0.0.1:9000 and wait
# until it serves, and make the bucket TEST_BUCKET_NAME. It takes any access key.
set -eu
docker run -d --name s3 -p 9000:8333 chrislusf/seaweedfs:latest server -s3 -dir=/data >/dev/null
for i in $(seq 1 60); do
  if curl -s -o /dev/null http://127.0.0.1:9000/; then
    echo "S3 serves."
    # The test bucket. SeaweedFS takes an anonymous create.
    curl -s -f -X PUT "http://127.0.0.1:9000/${TEST_BUCKET_NAME:-plugin-temporal-ci}" >/dev/null || true
    exit 0
  fi
  sleep 1
done
docker logs s3
echo "S3 did not start." >&2
exit 1
