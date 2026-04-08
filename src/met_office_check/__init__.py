import datetime
import urllib.parse
from pathlib import Path
from typing import Annotated

from httpx import Client
from obstore.store import AzureStore
from typer import Argument, Exit, Option, Typer

from . import aws, azure
from .model import Model

app = Typer()


@app.command()
def reference_datetime(
    model: Annotated[
        Model,
        Argument(
            help="Model to check",
        ),
    ],
    reference_datetime: Annotated[
        datetime.datetime | None,
        Option(
            "--reference-datetime",
            "-d",
            help="Forecast reference datetime (ISO 8601). If omitted, uses yesterday at midnight.",
            formats=["%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"],
        ),
    ] = None,
    directory: Annotated[
        Path,
        Argument(
            help="The data directory. If not provided, defaults to data",
        ),
    ] = Path("data"),
) -> None:
    """Check a single reference datetime"""
    if not directory.exists():
        print(f"{directory} does not exist")
        Exit(1)

    if reference_datetime is None:
        reference_datetime = datetime.datetime.combine(
            datetime.date.today() - datetime.timedelta(days=1),
            datetime.time(0, tzinfo=datetime.timezone.utc),
        )
    file_names = set(aws.get_file_names(model, reference_datetime))
    print(f"Found {len(file_names)} {model} files in AWS at {reference_datetime}")

    items = azure.get_items(directory, model, reference_datetime)
    print(
        f"Found {len(items)} {model} items in Azure (via stac-geoparquet in {directory}) at {reference_datetime}"
    )

    extra = set()
    for item in items:
        for asset in item.assets.values():
            file_name = asset.href.rsplit("/", 1)[1]
            if file_name.endswith(".nc"):
                try:
                    file_names.remove(file_name)
                except KeyError:
                    extra.add(file_name)
    print(f"{len(file_names)} in AWS but not in Azure")
    print(f"{len(extra)} in Azure but not in AWS")


@app.command()
def download_stac_geoparquet(
    model: Annotated[
        Model,
        Argument(
            help="Model to check",
        ),
    ],
    directory: Annotated[
        Path,
        Argument(
            help="The target directory. If not provided, defaults to data",
        ),
    ] = Path("data"),
) -> None:
    """Download all met office stac geoparquet files to the provided directory."""
    client = Client()
    collections = (
        client.get("https://planetarycomputer.microsoft.com/api/stac/v1/collections")
        .raise_for_status()
        .json()
    )
    for collection in collections["collections"]:
        if not collection["id"].startswith(f"met-office-{model}"):
            continue
        asset = collection["assets"]["geoparquet-items"]
        asset_href = urllib.parse.urlparse(asset["href"])
        geoparquet_account_name = asset["table:storage_options"]["account_name"]
        sas_key = (
            client.get(
                f"https://planetarycomputer.microsoft.com/api/sas/v1/token/{geoparquet_account_name}/{asset_href.netloc}"
            )
            .raise_for_status()
            .json()["token"]
        )
        store = AzureStore(
            account_name=geoparquet_account_name,
            container_name=asset_href.netloc,
            sas_key=sas_key,
        )
        for list_result in store.list(asset_href.path):
            for object in list_result:
                path = directory / object["path"]
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "wb") as f:
                    f.write(store.get(object["path"]).bytes())
