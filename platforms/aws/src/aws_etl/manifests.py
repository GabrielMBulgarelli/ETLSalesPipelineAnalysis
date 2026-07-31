"""Source discovery, identity calculation, and immutable manifest creation."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


SOURCE_FILES: dict[str, str] = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}

IMMUTABLE_MANIFEST_FIELDS = (
    "batch_id",
    "batch_timestamp",
    "dataset",
    "source_file_id",
    "content_sha256",
)


class DatasetValidationError(ValueError):
    pass


@dataclass(frozen=True)
class SourceFile:
    path: Path
    filename: str
    dataset: str
    file_size: int
    modification_timestamp: str
    content_sha256: str
    source_file_id: str


def discover_source_files(dataset_dir: str | Path) -> list[Path]:
    root = Path(dataset_dir).expanduser()
    if not root.exists():
        raise DatasetValidationError(f"DATASET_DIR does not exist: {root}")
    if not root.is_dir():
        raise DatasetValidationError(f"DATASET_DIR is not a directory: {root}")

    csv_files = sorted(
        (path for path in root.rglob("*") if path.is_file() and path.suffix.lower() == ".csv"),
        key=lambda path: str(path.relative_to(root)),
    )
    exact: dict[str, list[Path]] = {name: [] for name in SOURCE_FILES}
    ambiguous: list[Path] = []
    unexpected: list[Path] = []
    accepted_lower = {name.lower() for name in SOURCE_FILES}
    for path in csv_files:
        if path.name in exact:
            exact[path.name].append(path)
        elif path.name.lower() in accepted_lower:
            ambiguous.append(path)
        else:
            unexpected.append(path)

    missing = [name for name, matches in exact.items() if not matches]
    duplicates = {name: matches for name, matches in exact.items() if len(matches) > 1}
    if missing or duplicates or ambiguous or unexpected:
        lines = [f"DATASET_DIR validation failed: {root}"]
        if missing:
            lines.append("  missing canonical CSV files: " + ", ".join(sorted(missing)))
        if duplicates:
            lines.append("  duplicate canonical CSV files:")
            for name, matches in sorted(duplicates.items()):
                lines.append(f"    {name}: " + ", ".join(str(path.relative_to(root)) for path in matches))
        if ambiguous:
            lines.append("  ambiguous CSV filenames (canonical names are case-sensitive):")
            lines.extend(f"    {path.relative_to(root)}" for path in ambiguous)
        if unexpected:
            lines.append("  unexpected CSV files:")
            lines.extend(f"    {path.relative_to(root)}" for path in unexpected)
        raise DatasetValidationError("\n".join(lines))
    return [exact[name][0] for name in SOURCE_FILES]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def inspect_source_file(path: Path) -> SourceFile:
    stat = path.stat()
    filename = path.name
    dataset = SOURCE_FILES[filename]
    return SourceFile(
        path=path,
        filename=filename,
        dataset=dataset,
        file_size=stat.st_size,
        modification_timestamp=datetime.fromtimestamp(stat.st_mtime, UTC).isoformat().replace("+00:00", "Z"),
        content_sha256=_sha256(path),
        source_file_id=filename,
    )


def raw_object_key(source: SourceFile) -> str:
    return f"raw/{source.dataset}/content_sha256={source.content_sha256}/{source.filename}"


def create_manifest(
    source: SourceFile,
    batch_id: str,
    batch_timestamp: str,
    pipeline_version: str,
) -> dict[str, object]:
    return {
        "batch_id": batch_id,
        "batch_timestamp": batch_timestamp,
        "dataset": source.dataset,
        "source_file_id": source.source_file_id,
        "content_sha256": source.content_sha256,
        "source_object_path": raw_object_key(source),
        "pipeline_version": pipeline_version,
        "file_size": source.file_size,
        "source_modification_timestamp": source.modification_timestamp,
    }


def manifest_key(manifest_prefix: str, dataset: str, batch_id: str) -> str:
    return f"{manifest_prefix}dataset={dataset}/batch_id={batch_id}/manifest.json"


def immutable_identity_matches(first: dict[str, object], second: dict[str, object]) -> bool:
    return all(first.get(field) == second.get(field) for field in IMMUTABLE_MANIFEST_FIELDS)
