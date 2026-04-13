#!/bin/bash
# Build the sandbox_executor Lambda image for linux/arm64, push to ECR.
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URL="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/ceaser-sandbox-executor"
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
echo "==> Run 'cd ../../infra && terraform apply -var=deploy_lambda=true' to update Lambda"
