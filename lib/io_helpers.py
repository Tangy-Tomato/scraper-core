import json
import os
import tempfile
from pathlib import Path


def write_header(file_path, metadata, html):
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as target:
            target.write("<!--")
            target.write(json.dumps(metadata, ensure_ascii=False))
            target.write("-->\n")
            target.write(html)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temp_name, path)
        return True
    finally:
        Path(temp_name).unlink(missing_ok=True)


def read_header(file_path):
    with Path(file_path).open("r", encoding="utf-8", errors="replace") as source:
        first_line = source.readline().strip()
    if not first_line.startswith("<!--") or not first_line.endswith("-->"):
        return {}
    return json.loads(first_line[4:-3])


def append_order_id_atomic(file_path, order_id):
    path = Path(file_path)
    metadata = read_header(path)
    if not metadata:
        raise ValueError(f"HTML cache has no metadata header: {path}")
    with path.open("r", encoding="utf-8", errors="replace") as source:
        source.readline()
        html = source.read()
    order_ids = metadata.get("order_ids", [])
    if not isinstance(order_ids, list):
        order_ids = [order_ids] if order_ids else []
    order_id = str(order_id)
    if order_id not in order_ids:
        order_ids.append(order_id)
    metadata["order_ids"] = order_ids
    write_header(path, metadata, html)
