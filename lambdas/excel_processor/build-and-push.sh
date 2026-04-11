#!/bin/bash
# Build the excel_processor Lambda image for linux/arm64, push to ECR,
# then terraform apply with deploy_lambda=true to create/update the Lambda.
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URL="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/ceaser-excel-processor"
TAG="${1:-latest}"

cd "$(dirname "$0")"

echo "==> Logging in to ECR"
aws ecr get-login-password --region "$REGION" | \
  docker login --username AWS --password-stdin "$ECR_URL"

echo "==> Building linux/arm64 image"
docker buildx build \
  --platform linux/arm64 \
  --provenance=false \
  -t "${ECR_URL}:${TAG}" \
  --push \
  .

echo "==> Image pushed: ${ECR_URL}:${TAG}"

echo "==> Running terraform apply with deploy_lambda=true"
cd ../../infra
terraform apply -var='deploy_lambda=true' -auto-approve

echo "==> Done. Function: $(terraform output -raw lambda_function_name)"
