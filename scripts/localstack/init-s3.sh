#!/usr/bin/env bash
set -euo pipefail

BUCKET="${S3_BUCKET:-user-uploads}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "[init-s3] creating bucket ${BUCKET} in ${REGION}"
awslocal s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" 2>/dev/null \
  || echo "[init-s3] bucket ${BUCKET} already exists"

echo "[init-s3] applying CORS policy"
awslocal s3api put-bucket-cors --bucket "${BUCKET}" --cors-configuration '{
  "CORSRules": [
    {
      "AllowedHeaders": ["*"],
      "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
      "AllowedOrigins": ["*"],
      "ExposeHeaders": ["ETag", "x-amz-request-id", "x-amz-version-id"],
      "MaxAgeSeconds": 3000
    }
  ]
}'

echo "[init-s3] applying lifecycle rules"
awslocal s3api put-bucket-lifecycle-configuration --bucket "${BUCKET}" --lifecycle-configuration '{
  "Rules": [
    {
      "ID": "abort-incomplete-multipart-uploads",
      "Status": "Enabled",
      "Filter": {"Prefix": ""},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}
    }
  ]
}' || echo "[init-s3] lifecycle configuration not supported by this LocalStack build; skipping"

echo "[init-s3] done - buckets:"
awslocal s3 ls
