"""R1-03: copy the postgres_oltp raw history under the olist_synth_cdc prefix.

Every object under ``raw/postgres_oltp/`` and ``quarantine/postgres_oltp/``
is copied to the same key under the new source id. The copy keeps the
payload bytes, the execution id in the key, the ``_ingestion_ts`` metadata,
and the content type; it rewrites only the ``source_id`` that Bronze checks
against the key: the object's ``source_id`` user metadata and the
``source_id`` field of each ``_COMPLETE`` marker. A re-run overwrites the
copies with identical bytes, so the ETags stay the same.

The output is counts and names only; no payload is read except the markers,
which hold names and counts.

    set -a; . ./.env.example; . ./.env; set +a
    uv run python scripts/r1_03_copy_raw.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter

from open_data_platform.object_storage import client_from_env

OLD = "postgres_oltp"
NEW = "olist_synth_cdc"
PREFIXES = ("raw", "quarantine")
MARKER = "_COMPLETE"


def _keys(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return keys


def _new_metadata(metadata: dict[str, str], key: str) -> dict[str, str]:
    if metadata.get("source_id") not in (None, OLD):
        msg = f"{key}: source_id metadata is not {OLD}"
        raise SystemExit(msg)
    return {**metadata, "source_id": NEW} if "source_id" in metadata else metadata


def copy_all(client, bucket: str, *, dry_run: bool) -> Counter[str]:
    counts: Counter[str] = Counter()
    for top in PREFIXES:
        for key in _keys(client, bucket, f"{top}/{OLD}/"):
            new_key = f"{top}/{NEW}/" + key.removeprefix(f"{top}/{OLD}/")
            dataset = key.split("/")[2]
            head = client.head_object(Bucket=bucket, Key=key)
            metadata = _new_metadata(head.get("Metadata") or {}, key)
            content_type = head.get("ContentType", "application/octet-stream")
            kind = "marker" if key.endswith(f"/{MARKER}") else "object"
            counts[f"{top}\t{dataset}\t{kind}"] += 1
            if dry_run:
                continue
            if kind == "marker":
                body = json.loads(
                    client.get_object(Bucket=bucket, Key=key)["Body"].read()
                )
                if not isinstance(body, dict) or body.get("source_id") != OLD:
                    msg = f"{key}: marker is not an object with source_id {OLD}"
                    raise SystemExit(msg)
                body["source_id"] = NEW
                client.put_object(
                    Bucket=bucket,
                    Key=new_key,
                    Body=json.dumps(body, sort_keys=True).encode("utf-8"),
                    Metadata=metadata,
                    ContentType=content_type,
                )
            else:
                client.copy_object(
                    Bucket=bucket,
                    Key=new_key,
                    CopySource={"Bucket": bucket, "Key": key},
                    Metadata=metadata,
                    MetadataDirective="REPLACE",
                    ContentType=content_type,
                )
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    bucket = os.environ["ODP_RAW_BUCKET"]
    counts = copy_all(client_from_env(os.environ), bucket, dry_run=args.dry_run)
    for name, count in sorted(counts.items()):
        print(f"{name}\t{count}")
    print(f"total\t{sum(counts.values())}\t{'dry-run' if args.dry_run else 'copied'}")


if __name__ == "__main__":
    main()
