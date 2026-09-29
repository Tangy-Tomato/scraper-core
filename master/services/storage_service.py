import asyncio
import gzip
import os
import tempfile
from collections.abc import AsyncIterable
from pathlib import Path
from uuid import UUID


class UploadTooLargeError(ValueError):
    pass


class InvalidPayloadError(ValueError):
    pass


async def save_payload(
    chunks: AsyncIterable[bytes],
    task_id: UUID,
    storage_dir: Path,
    max_bytes: int,
    max_uncompressed_bytes: int,
) -> Path:
    storage_dir.mkdir(parents=True, exist_ok=True)
    final_path = storage_dir / f"{task_id}.html.gz"
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{task_id}.", suffix=".tmp", dir=storage_dir
    )
    temp_path = Path(temp_name)
    total = 0
    try:
        with os.fdopen(fd, "wb") as target:
            async for chunk in chunks:
                total += len(chunk)
                if total > max_bytes:
                    raise UploadTooLargeError(
                        f"compressed payload exceeds {max_bytes} bytes"
                    )
                await asyncio.to_thread(target.write, chunk)
            await asyncio.to_thread(target.flush)
            await asyncio.to_thread(os.fsync, target.fileno())
        await asyncio.to_thread(
            _validate_gzip, temp_path, max_uncompressed_bytes
        )
        os.replace(temp_path, final_path)
        return final_path
    finally:
        temp_path.unlink(missing_ok=True)


def _validate_gzip(path: Path, max_uncompressed_bytes: int) -> None:
    try:
        with gzip.open(path, "rb") as source:
            decompressed = 0
            while chunk := source.read(1024 * 1024):
                decompressed += len(chunk)
                if decompressed > max_uncompressed_bytes:
                    raise UploadTooLargeError(
                        "uncompressed HTML exceeds "
                        f"{max_uncompressed_bytes} bytes"
                    )
    except (gzip.BadGzipFile, EOFError, OSError) as exc:
        raise InvalidPayloadError("submission must contain valid gzip data") from exc
