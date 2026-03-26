"""Supabase Storage backend — production cloud file storage.

Files are uploaded to a Supabase Storage bucket. For reads, signed URLs
are generated so pandas can read parquet/CSV directly from the URL
without downloading to disk first.
"""

from __future__ import annotations

import logging
from pathlib import Path

import httpx

from app.services.storage.base import StorageBackend

logger = logging.getLogger(__name__)

# Signed URL validity (seconds)
_SIGNED_URL_EXPIRY = 600  # 10 minutes


class SupabaseStorage(StorageBackend):
    """Store files in a Supabase Storage bucket."""

    def __init__(self, url: str, service_key: str, bucket: str):
        self._url = url.rstrip("/")
        self._key = service_key
        self._bucket = bucket
        self._storage_url = f"{self._url}/storage/v1"

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._key}",
            "apikey": self._key,
        }

    async def upload(self, data: bytes, remote_path: str) -> str:
        """Upload bytes to Supabase bucket. Returns remote_path as the key."""
        ext = Path(remote_path).suffix.lower()
        content_types = {
            ".csv": "text/csv",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls": "application/vnd.ms-excel",
            ".parquet": "application/octet-stream",
        }
        content_type = content_types.get(ext, "application/octet-stream")

        url = f"{self._storage_url}/object/{self._bucket}/{remote_path}"

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                url,
                headers={
                    **self._headers(),
                    "Content-Type": content_type,
                    "x-upsert": "true",
                },
                content=data,
            )
            if resp.status_code not in (200, 201):
                logger.error("Supabase upload failed: %s %s", resp.status_code, resp.text[:300])
                raise RuntimeError(f"Supabase upload failed: {resp.status_code}")

        logger.info("Supabase upload: %s (%d bytes)", remote_path, len(data))
        return remote_path

    async def download_url(self, remote_path: str) -> str:
        """Generate a signed URL for direct pandas reads.

        Returns a URL like:
          https://xxx.supabase.co/storage/v1/object/sign/bucket/path?token=...
        """
        url = f"{self._storage_url}/object/sign/{self._bucket}/{remote_path}"

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                url,
                headers=self._headers(),
                json={"expiresIn": _SIGNED_URL_EXPIRY},
            )
            if resp.status_code != 200:
                logger.error("Supabase sign failed: %s %s", resp.status_code, resp.text[:200])
                raise RuntimeError(f"Failed to generate signed URL: {resp.status_code}")

        data = resp.json()
        signed_path = data.get("signedURL", "")
        # signedURL is relative like /object/sign/... — needs /storage/v1 prefix
        if signed_path.startswith("/"):
            return f"{self._url}/storage/v1{signed_path}"
        return signed_path

    async def delete(self, remote_path: str) -> None:
        """Delete from Supabase bucket."""
        url = f"{self._storage_url}/object/{self._bucket}"

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(
                "DELETE",
                url,
                headers=self._headers(),
                json={"prefixes": [remote_path]},
            )
            if resp.status_code not in (200, 204, 404):
                logger.warning("Supabase delete failed: %s", resp.status_code)

        logger.info("Supabase delete: %s", remote_path)
