"""S3 storage backend.

Uses the IAM role attached to the EC2 instance — no credentials in code.
`download_url` returns a presigned GET URL that pandas can read directly;
callers that need a local path should download the bytes themselves.
"""

from __future__ import annotations

import logging
from pathlib import Path

from app.services.storage.base import StorageBackend

logger = logging.getLogger(__name__)

_SIGNED_URL_EXPIRY = 900  # 15 minutes — short-lived to limit exposure if URL is shared


class S3Storage(StorageBackend):
    def __init__(self, bucket: str, region: str = "us-east-1"):
        import boto3

        self._bucket = bucket
        self._region = region
        self._client = boto3.client("s3", region_name=region)

    def _content_type(self, remote_path: str) -> str:
        ext = Path(remote_path).suffix.lower()
        return {
            ".csv": "text/csv",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls": "application/vnd.ms-excel",
            ".parquet": "application/octet-stream",
        }.get(ext, "application/octet-stream")

    async def upload(self, data: bytes, remote_path: str) -> str:
        import asyncio

        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self._bucket,
            Key=remote_path,
            Body=data,
            ContentType=self._content_type(remote_path),
        )
        logger.info("S3 upload: s3://%s/%s (%d bytes)", self._bucket, remote_path, len(data))
        return remote_path

    async def upload_file(self, local_path: str, remote_path: str) -> str:
        """Upload directly from a local file — avoids loading full bytes into memory.

        Not part of the abstract interface but available for large-file paths.
        """
        import asyncio

        await asyncio.to_thread(
            self._client.upload_file,
            local_path,
            self._bucket,
            remote_path,
            ExtraArgs={"ContentType": self._content_type(remote_path)},
        )
        logger.info("S3 upload (file): s3://%s/%s", self._bucket, remote_path)
        return remote_path

    async def download_url(self, remote_path: str) -> str:
        import asyncio
        from urllib.parse import quote

        # URL-encode the key (spaces → %20) while preserving path separators.
        # Boto3 presigned URLs need the encoded key to produce valid HTTP URLs.
        encoded_key = quote(remote_path, safe="/")
        url = await asyncio.to_thread(
            self._client.generate_presigned_url,
            "get_object",
            Params={"Bucket": self._bucket, "Key": encoded_key},
            ExpiresIn=_SIGNED_URL_EXPIRY,
        )
        return url

    async def delete(self, remote_path: str) -> None:
        import asyncio

        await asyncio.to_thread(self._client.delete_object, Bucket=self._bucket, Key=remote_path)
        logger.info("S3 delete: s3://%s/%s", self._bucket, remote_path)
