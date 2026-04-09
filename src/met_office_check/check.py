import datetime
from pathlib import Path

import pandas as pd
from pydantic import BaseModel
from stactools.met_office_deterministic.href import Href

from .model import Model


class Check(BaseModel):
    model: Model
    reference_datetime: datetime.datetime
    missing: list[str]
    extra: dict[str, dict[str, list[str]]]

    def is_ok(self) -> bool:
        return not self.missing and not self.extra


def write_parquet(checks: list[Check], path: Path) -> None:
    rows = []
    for c in checks:
        for s3_path in c.missing:
            href = Href.parse(s3_path)
            rows.append(
                {
                    "model": str(c.model),
                    "reference_datetime": c.reference_datetime,
                    "path": s3_path,
                    "collection": href.collection_id,
                    "item": href.item_id,
                }
            )
    df = pd.DataFrame(
        rows, columns=["model", "reference_datetime", "path", "collection", "item"]
    )
    df.to_parquet(path, index=False)
