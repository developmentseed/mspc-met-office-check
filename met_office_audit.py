from __future__ import annotations

import asyncio
from asyncio import TaskGroup
from collections import defaultdict
from dataclasses import dataclass

import httpx
import numpy
from matplotlib import pyplot
from obstore.store import AzureStore, S3Store
from stactools.met_office_deterministic.constants import Theme


@dataclass(frozen=True)
class MetOfficePath:
    interval: str
    variable: str

    @classmethod
    def parse(cls, path: str) -> MetOfficePath:
        file_name = path.split("/")[-1]
        parts = file_name.split("-")
        return MetOfficePath(
            interval=parts[1], variable="-".join(parts[2:]).split(".")[0]
        )


@dataclass(frozen=True)
class MicrosoftPrefix:
    collection: str
    year: int
    month: int

    def to_str(self) -> str:
        return f"{self.collection}/{self.year}{self.month:02d}01T0000Z"


@dataclass(frozen=True)
class AwsPrefix:
    year: int
    month: int

    def to_str(self) -> str:
        return f"{self.year}{self.month:02d}01T0000Z"


def year_month_prefixes() -> list[tuple[int, int]]:
    result = []
    for year in [2024, 2025, 2026]:
        for month in range(1, 13):
            if year == 2026 and month > 3:
                continue
            result.append((year, month))
    return result


async def count_microsoft(
    store: AzureStore, prefix: MicrosoftPrefix
) -> dict[tuple[MicrosoftPrefix, str], int]:
    result: dict[tuple[MicrosoftPrefix, str], int] = defaultdict(int)
    async for list_result in store.list(prefix=prefix.to_str()):
        for object_meta in list_result:
            if object_meta["path"].endswith(".updated"):
                continue
            path = MetOfficePath.parse(object_meta["path"])
            result[(prefix, path.interval)] += 1
    return result


async def count_aws(
    store: S3Store, prefix: AwsPrefix
) -> dict[tuple[AwsPrefix, str], int]:
    result: dict[tuple[AwsPrefix, str], int] = defaultdict(int)
    async for list_result in store.list(prefix=prefix.to_str()):
        for object_meta in list_result:
            if object_meta["path"].endswith(".updated"):
                continue
            path = MetOfficePath.parse(object_meta["path"])
            try:
                if Theme.from_parameter(path.variable) == Theme.near_surface:
                    result[(prefix, path.interval)] += 1
            except ValueError:
                pass
    return result


def plot_bar_chart(
    counts: dict,
    prefixes: list,
    year_months: list[tuple[int, int]],
    title: str,
) -> None:
    intervals = sorted({interval for _, interval in counts})
    labels = [f"{y}-{m:02d}" for y, m in year_months]
    x = numpy.arange(len(year_months))
    bottom = numpy.zeros(len(year_months))

    figure, axis = pyplot.subplots(figsize=(16, 6))

    for interval in intervals:
        values = numpy.array(
            [
                sum(
                    counts.get((p, interval), 0)
                    for p in prefixes
                    if (p.year, p.month) == ym
                )
                for ym in year_months
            ]
        )
        axis.bar(x, values, bottom=bottom, label=interval)
        bottom += values

    axis.set_xticks(x)
    axis.set_xticklabels(labels, rotation=45, ha="right")
    axis.set_ylabel("Asset count")
    axis.set_title(title)
    axis.legend()
    pyplot.tight_layout()
    pyplot.show()


def plot_microsoft_by_collection(
    counts: dict,
    prefixes: list[MicrosoftPrefix],
    year_months: list[tuple[int, int]],
) -> None:
    intervals = sorted({interval for _, interval in counts})
    labels = [f"{y}-{m:02d}" for y, m in year_months]
    x = numpy.arange(len(year_months))
    collections = ["near-surface", "pressure", "height", "whole-atmosphere"]

    figure, axes = pyplot.subplots(2, 2, figsize=(16, 10), sharey=True)

    for ax, collection in zip(axes.flat, collections):
        bottom = numpy.zeros(len(year_months))
        for interval in intervals:
            values = numpy.array(
                [
                    sum(
                        counts.get((p, interval), 0)
                        for p in prefixes
                        if (p.year, p.month) == ym and p.collection == collection
                    )
                    for ym in year_months
                ]
            )
            ax.bar(x, values, bottom=bottom, label=interval)
            bottom += values
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_ylabel("Asset count")
        ax.set_title(collection)

    pyplot.tight_layout()
    pyplot.show()


async def main() -> None:
    ym_list = year_month_prefixes()

    # Microsoft
    sas_key = httpx.get(
        "https://planetarycomputer.microsoft.com/api/sas/v1/token/ukmoeuwest/deterministic"
    ).json()["token"]
    azure_store = AzureStore(
        account_name="ukmoeuwest",
        container_name="deterministic",
        sas_key=sas_key,
        prefix="global",
    )

    ms_prefixes: list[MicrosoftPrefix] = []
    for year, month in ym_list:
        for collection in ["near-surface", "pressure", "height", "whole-atmosphere"]:
            ms_prefixes.append(
                MicrosoftPrefix(collection=collection, year=year, month=month)
            )

    ms_counts: defaultdict[tuple[MicrosoftPrefix, str], int] = defaultdict(int)
    async with TaskGroup() as task_group:
        tasks = {
            task_group.create_task(count_microsoft(azure_store, p)): p
            for p in ms_prefixes
        }
    for task in tasks:
        for key, value in task.result().items():
            ms_counts[key] += value

    plot_bar_chart(
        ms_counts, ms_prefixes, ym_list, "Met Office assets by month (all collections)"
    )
    plot_microsoft_by_collection(ms_counts, ms_prefixes, ym_list)

    # AWS
    aws_store = S3Store(
        bucket="met-office-atmospheric-model-data",
        region="eu-west-2",
        skip_signature=True,
        prefix="global-deterministic-10km",
    )

    aws_prefixes = [AwsPrefix(year=y, month=m) for y, m in ym_list]

    aws_counts: defaultdict[tuple[AwsPrefix, str], int] = defaultdict(int)
    async with TaskGroup() as task_group:
        tasks = {
            task_group.create_task(count_aws(aws_store, p)): p for p in aws_prefixes
        }
    for task in tasks:
        for key, value in task.result().items():
            aws_counts[key] += value

    plot_bar_chart(aws_counts, aws_prefixes, ym_list, "Met Office assets by month")

    # Near-surface variable comparison (March 2026)
    azure_variables: defaultdict[str, set[str]] = defaultdict(set)
    async for list_result in azure_store.list(prefix="near-surface/20260301T0000Z"):
        for object_meta in list_result:
            if object_meta["path"].endswith(".updated"):
                continue
            path = MetOfficePath.parse(object_meta["path"])
            azure_variables[path.interval].add(path.variable)

    aws_variables: defaultdict[str, set[str]] = defaultdict(set)
    async for list_result in aws_store.list(prefix="20260301T0000Z"):
        for object_meta in list_result:
            if object_meta["path"].endswith(".updated"):
                continue
            path = MetOfficePath.parse(object_meta["path"])
            try:
                if Theme.from_parameter(path.variable) == Theme.near_surface:
                    aws_variables[path.interval].add(path.variable)
            except ValueError:
                pass

    azure_only: defaultdict[str, list[str]] = defaultdict(list)
    all_intervals = sorted(set(azure_variables) | set(aws_variables))

    for interval in all_intervals:
        azure_vars = azure_variables.get(interval, set())
        aws_vars = aws_variables.get(interval, set())
        for variable in sorted(azure_vars - aws_vars):
            azure_only[variable].append(interval)

    for variable, intervals in sorted(azure_only.items()):
        print(f"{variable}: {', '.join(intervals)}")


if __name__ == "__main__":
    asyncio.run(main())
