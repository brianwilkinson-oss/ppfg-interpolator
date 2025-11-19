"""Helpers for loading dataset metadata."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
from typing import Iterable, List, Optional


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
        )
        for item in data
        if item.get("provider") == "corva" and item.get("company_id") == company_id
    ]
    return metas


__all__ = ["DatasetMeta", "load_corva_company_datasets"]
