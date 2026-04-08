import datetime

from obstore.store import S3Store

from .model import Model


def get_file_names(model: Model, reference_datetime: datetime.datetime) -> list[str]:
    forecast_prefix = reference_datetime.strftime("%Y%m%dT%H%MZ")
    s3_store = S3Store(
        bucket="met-office-atmospheric-model-data",
        region="eu-west-2",
        skip_signature=True,
        prefix=model.s3_prefix,
    )
    file_names = []
    for list_result in s3_store.list(prefix=forecast_prefix):
        for object in list_result:
            file_names.append(object["path"].rsplit("/", 1)[1])
    return file_names
