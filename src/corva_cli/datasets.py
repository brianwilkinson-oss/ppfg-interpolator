"""Helpers for loading dataset metadata."""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DATASET_FILE = Path(__file__).resolve().parents[2] / "docs" / "dataset.json"


@dataclass(frozen=True)
class DatasetMeta:
    name: str
    friendly_name: str
    provider: str
    company_id: int
    data_type: str
    description: str
    dataset: str
    indexes: Tuple[Tuple[str, ...], ...] = field(default_factory=tuple)

    @property
    def requires_time(self) -> bool:
        return self.data_type in {"time", "timeseries"}

    @property
    def requires_depth(self) -> bool:
        return self.data_type == "depth"

    @property
    def slug(self) -> str:
        base = self.friendly_name or self.dataset
        slug = "".join(ch if ch.isalnum() else "-" for ch in base.lower()).strip("-")
        return slug or f"dataset-{self.company_id}-{self.dataset}"


def _normalize_index_field(field_name: str) -> Optional[str]:
    lowered = field_name.lower()
    tail = lowered.split(".")[-1]
    if tail in {"asset_id", "company_id"}:
        return tail
    if tail in {"timestamp", "start_time", "end_time"}:
        return tail
    if "depth" in tail:
        return "depth"
    if "depth" in lowered:
        return "depth"
    return None


def _extract_index_fields(item: Dict[str, Any]) -> Tuple[Tuple[str, ...], ...]:
    indexes_data = item.get("indexes", []) or []
    relevant: List[Tuple[str, ...]] = []
    for index_entry in indexes_data:
        fields_data = index_entry.get("fields", []) or []
        raw_fields: List[str] = []
        for field in fields_data:
            if not isinstance(field, dict):
                continue
            name = next(iter(field.keys()), None)
            if not name:
                continue
            raw_fields.append(name)
        if raw_fields:
            relevant.append(tuple(raw_fields))
    return tuple(relevant)


@lru_cache()
def load_corva_company_datasets(company_id: int = 3) -> List[DatasetMeta]:
    if not DATASET_FILE.exists():
        return []
    data = json.loads(DATASET_FILE.read_text())
    metas = [
        DatasetMeta(
            name=item["name"],
            friendly_name=item.get("friendly_name") or item["name"],
            provider=item.get("provider", ""),
            company_id=item.get("company_id", 0),
            data_type=item.get("data_type", "reference"),
            description=item.get("description", ""),
            dataset=item["name"].split("#")[-1],
            indexes=_extract_index_fields(item),
        )
        for item in data
        if item.get("provider") == "corva" and item.get("company_id") == company_id
    ]
    return metas


__all__ = ["DatasetMeta", "load_corva_company_datasets"]
