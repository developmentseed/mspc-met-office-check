import datetime
from pathlib import Path

from pystac import Item
from rustac import DuckdbClient

from .model import Model


def get_items(
    directory: Path, model: Model, reference_datetime: datetime.datetime
) -> list[Item]:
    client = DuckdbClient()
    return [
        Item.from_dict(item)
        for item in client.search(
            str(directory.resolve() / f"met-office-{model}-deterministic-*.parquet"),
            filter={
                "op": "=",
                "args": [
                    {"property": "forecast:reference_datetime"},
                    reference_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ],
            },
        )
    ]
