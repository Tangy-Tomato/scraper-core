import hashlib


def generate_id(text):
    if not text:
        return "UNKNOWN"
    return hashlib.md5(str(text).strip().encode("utf-8")).hexdigest()
