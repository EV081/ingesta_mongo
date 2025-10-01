import json
import os
import sys
import time
import csv
from datetime import datetime
from typing import List, Optional
import boto3
import pandas as pd
from pymongo import MongoClient
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from bson import ObjectId

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB   = os.getenv("MONGO_DB", "")
MONGO_USER = os.getenv("MONGO_USER", "")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
MONGO_AUTH_DB = os.getenv("MONGO_AUTH_DB", "admin")

COLLECTIONS_ENV = os.getenv("COLLECTIONS", "")
COLLECTIONS: List[str] = [c.strip() for c in COLLECTIONS_ENV.split(",") if c.strip()]

CSV_SEP = os.getenv("CSV_SEP", ",")
CSV_QUOTE = os.getenv("CSV_QUOTE", "MINIMAL").upper()
CSV_LINE_TERMINATOR = os.getenv("CSV_LINE_TERMINATOR", "\n")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/out")

S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1")

TIMESTAMP = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def csv_quote_const(name: str):
    import csv as _csv
    return {
        "MINIMAL": _csv.QUOTE_MINIMAL,
        "ALL": _csv.QUOTE_ALL,
        "NONNUMERIC": _csv.QUOTE_NONNUMERIC,
        "NONE": _csv.QUOTE_NONE,
    }.get(name, _csv.QUOTE_MINIMAL)


def ensure_output_dir(path: str):
    os.makedirs(path, exist_ok=True)


def get_client() -> MongoClient:
    if MONGO_USER and MONGO_PASSWORD:
        uri = (
            f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}"
            f"@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_DB}"
        )
    else:
        uri = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"
    return MongoClient(uri)


def convert_objectid_to_str(doc):
    """Convierte todos los ObjectId en un documento a su representación en string"""
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            doc[key] = str(value)
        elif isinstance(value, dict):
            convert_objectid_to_str(value) 
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    convert_objectid_to_str(item)  
    return doc


def export_collection_to_ndjson(client: MongoClient, collection_name: str, out_dir: str) -> str:
    """Exporta una colección a NDJSON (newline-delimited JSON)."""
    db = client[MONGO_DB]
    coll = db[collection_name]

    cursor = coll.find({})
    docs = list(cursor)

    if not docs:
        print(f"[WARN] La colección '{collection_name}' está vacía. Se omite.")
        return ""

    filename = f"{collection_name}_{TIMESTAMP}.ndjson"
    out_path = os.path.join(out_dir, filename)

    with open(out_path, "w", encoding="utf-8") as f:
        for doc in docs:
            doc = convert_objectid_to_str(doc)  
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print(f"[OK] {collection_name} -> {out_path} ({len(docs)} documentos)")
    return out_path


def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def upload_to_s3(local_path: str, bucket: str, prefix: Optional[str]) -> str:
    key = os.path.basename(local_path)
    if prefix:
        key = f"{prefix.rstrip('/')}/{key}"

    cli = s3_client()
    try:
        cli.upload_file(local_path, bucket, key)
    except (NoCredentialsError, PartialCredentialsError):
        print("[ERROR] Credenciales de AWS no encontradas o incompletas.", file=sys.stderr)
        raise
    except ClientError as e:
        print(f"[ERROR] Fallo subiendo a S3: {e}", file=sys.stderr)
        raise

    print(f"[OK] Subido a s3://{bucket}/{key}")
    return key


def main():
    if not MONGO_DB or not COLLECTIONS:
        print(
            "Faltan variables de entorno obligatorias: MONGO_DB y COLLECTIONS.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not S3_BUCKET:
        print("Falta S3_BUCKET.", file=sys.stderr)
        sys.exit(1)

    ensure_output_dir(OUTPUT_DIR)
    client = get_client()

    exported_files = []
    for coll in COLLECTIONS:
        path = export_collection_to_ndjson(client, coll, OUTPUT_DIR) 
        if path:
            exported_files.append(path)

    if not exported_files:
        print("[INFO] No se exportó ninguna colección. Revisa nombres y permisos.", file=sys.stderr)
        sys.exit(2)

    for path in exported_files:
        upload_to_s3(path, S3_BUCKET, S3_PREFIX)

    print("[DONE] Ingesta completada.")


if __name__ == "__main__":
    start = time.time()
    try:
        main()
    finally:
        dur = time.time() - start
        print(f"Duración total: {dur:.1f}s")
