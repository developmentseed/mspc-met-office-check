from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import httpx
import numpy
import tqdm
from matplotlib import pyplot
from obstore.store import AzureStore

sas_key = httpx.get(
    "https://planetarycomputer.microsoft.com/api/sas/v1/token/ukmoeuwest/deterministic"
).json()["token"]

store = AzureStore(
    account_name="ukmoeuwest",
    container_name="deterministic",
    sas_key=sas_key,
    prefix="global/near-surface",
)

prefixes = []
for year in [2024, 2025, 2026]:
    for month in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
        if year == 2026 and month > 3:
            continue
        prefixes.append(f"{year}{month:02d}01T0000Z")


@dataclass(frozen=True)
class MetOffcePath:
    interval: str
    variable: str

    @classmethod
    def parse(cls, path: str) -> MetOffcePath:
        file_name = path.split("/")[-1]
        parts = file_name.split("-")
        return MetOffcePath(interval=parts[1], variable=parts[2].split(".")[0])


def count_prefix(prefix: str) -> dict[tuple[str, str], int]:
    result: dict[tuple[str, str], int] = defaultdict(int)
    for list_result in store.list(prefix=prefix):
        for object_meta in list_result:
            if object_meta["path"].endswith(".updated"):
                continue
            path = MetOffcePath.parse(object_meta["path"])
            result[(prefix, path.interval)] += 1
    return result


counts: defaultdict[tuple[str, str], int] = defaultdict(int)

with ThreadPoolExecutor() as pool:
    futures = {pool.submit(count_prefix, prefix): prefix for prefix in prefixes}
    for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
        for key, value in future.result().items():
            counts[key] += value

intervals = sorted({interval for _, interval in counts})
labels = [prefix[0:6] for prefix in prefixes]
x = numpy.arange(len(prefixes))
bottom = numpy.zeros(len(prefixes))

fig, ax = pyplot.subplots(figsize=(16, 8))

for interval in intervals:
    values = numpy.array([counts.get((prefix, interval), 0) for prefix in prefixes])
    ax.bar(x, values, bottom=bottom, label=interval[3:6])
    bottom += values

ax.set_xticks(x)
ax.set_xticklabels(labels, rotation=45, ha="right")
ax.set_ylabel("Asset count")
ax.set_title("Met Office near-surface assets by month and interval")
ax.legend(title="Interval", bbox_to_anchor=(1.05, 1), loc="upper left")
fig.tight_layout()
fig.savefig("met_office_check.png", dpi=150)
print("Saved met_office_check.png")
