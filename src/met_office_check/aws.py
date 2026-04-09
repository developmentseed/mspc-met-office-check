import asyncio
import datetime
from asyncio import Queue, Semaphore, TaskGroup
from collections import defaultdict
from pathlib import Path

import tqdm
from obstore.store import S3Store

from . import azure
from .check import Check
from .model import Model

BUCKET = "met-office-atmospheric-model-data"


class Store:
    def __init__(self, model: Model) -> None:
        self.store = S3Store(
            bucket=BUCKET,
            region="eu-west-2",
            skip_signature=True,
            prefix=model.s3_prefix,
        )
        self.model = model
        self.semaphore = Semaphore(10)
        self.queue = Queue()

    def get_reference_datetimes(self) -> list[datetime.datetime]:
        reference_datetimes = list()
        for prefix in tqdm.tqdm(
            self.store.list_with_delimiter()["common_prefixes"],
            desc="Reference datetimes",
        ):
            reference_datetimes.append(
                datetime.datetime.strptime(prefix, "%Y%m%dT%H%MZ")
            )
        return reference_datetimes

    async def get_s3_paths(self, reference_datetime: datetime.datetime) -> list[str]:
        forecast_prefix = reference_datetime.strftime("%Y%m%dT%H%MZ")
        prefix = self.store.prefix
        paths = []
        async for list_result in self.store.list_async(prefix=forecast_prefix):
            for object in list_result:
                paths.append(f"s3://{BUCKET}/{prefix}/{object['path']}")
        return paths

    async def check(
        self,
        reference_datetime: datetime.datetime,
        directory: Path,
    ) -> Check | None:
        async with self.semaphore:
            s3_paths = set(await self.get_s3_paths(reference_datetime))
        await self.queue.put(reference_datetime)

        path_by_file_name = {p.rsplit("/", 1)[1]: p for p in s3_paths}
        items = azure.get_items(directory, self.model, reference_datetime)
        if not items:
            return None
        extra = defaultdict(lambda: defaultdict(list))
        for item in items:
            assert item.collection_id
            for asset in item.assets.values():
                file_name = asset.href.rsplit("/", 1)[1]
                if file_name.endswith(".nc"):
                    s3_path = path_by_file_name.get(file_name)
                    if s3_path and s3_path in s3_paths:
                        s3_paths.remove(s3_path)
                    else:
                        extra[item.collection_id][item.id].append(file_name)
        return Check(
            model=self.model,
            reference_datetime=reference_datetime,
            missing=sorted(s3_paths),
            extra=dict(extra),
        )

    async def check_all(self, directory: Path) -> list[Check]:
        reference_datetimes = self.get_reference_datetimes()
        progress = asyncio.create_task(self.progress(len(reference_datetimes)))
        async with TaskGroup() as task_group:
            tasks = [
                task_group.create_task(self.check(reference_datetime, directory))
                for reference_datetime in reference_datetimes
            ]
        checks = list()
        for task in tasks:
            if check := task.result():
                if not check.is_ok():
                    checks.append(check)
        progress.cancel()
        return checks

    async def progress(self, total: int) -> None:
        progress = tqdm.tqdm(total=total, desc="Checks")
        try:
            while True:
                _ = await self.queue.get()
                progress.update()
        finally:
            progress.close()
