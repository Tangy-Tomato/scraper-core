import gzip
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from master.services.storage_service import (
    InvalidPayloadError,
    UploadTooLargeError,
    save_payload,
)


class StorageIdempotencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_overwrites_canonical_file_atomically(self):
        task_id = uuid4()
        with tempfile.TemporaryDirectory() as directory:
            storage_dir = Path(directory)
            first = gzip.compress(b"first payload")
            second = gzip.compress(b"retry payload")
            async def chunks(payload):
                yield payload

            await save_payload(chunks(first), task_id, storage_dir, 1024, 1024)
            path = await save_payload(chunks(second), task_id, storage_dir, 1024, 1024)
            self.assertEqual(gzip.decompress(path.read_bytes()), b"retry payload")
            self.assertEqual(list(storage_dir.glob("*.tmp")), [])

    async def test_rejects_non_gzip_data_without_leaving_temp_file(self):
        task_id = uuid4()
        with tempfile.TemporaryDirectory() as directory:
            storage_dir = Path(directory)
            with self.assertRaises(InvalidPayloadError):
                async def chunks():
                    yield b"not gzip"

                await save_payload(chunks(), task_id, storage_dir, 1024, 1024)
            self.assertEqual(list(storage_dir.iterdir()), [])

    async def test_rejects_gzip_payload_over_uncompressed_limit(self):
        task_id = uuid4()
        with tempfile.TemporaryDirectory() as directory:
            storage_dir = Path(directory)

            async def chunks():
                yield gzip.compress(b"payload is too large")

            with self.assertRaises(UploadTooLargeError):
                await save_payload(chunks(), task_id, storage_dir, 1024, 4)
            self.assertEqual(list(storage_dir.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
