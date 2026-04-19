import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class APIMetadata:
    title: str
    version: str
    description: str
    license: str
    license_url: str
    api_base_url: str
    data_provider: str
    developer: str
    developer_github: str
    developer_note: str
    support_email: str
    support_name: str
    support_url: str
    metadata_date: datetime.date
