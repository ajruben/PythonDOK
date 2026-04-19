"""python module to call OGC API Features service for Nationaal Wegen Bestand (NWB) - Wegen."""
from __future__ import annotations

import datetime
from dataclasses import fields
from typing import Any

from urllib.parse import urljoin

from .metadata import APIMetadata

# --- API + dataset metadata  ---
__title__ = "NWB - Wegen"
__license__ = "CC0 1.0"
__license_url__ = "http://creativecommons.org/publicdomain/zero/1.0/deed.nl"
__api_support__ = "https://www.pdok.nl/support"
__data_provider__ = "Rijkswaterstaat (RWS)"
__api_version__ = "1.0.0"
__url__ = "https://api.pdok.nl/rws/nationaal-wegenbestand-wegen/ogc/v1"
__description__ = (
    "OGC API Features voor het Nationaal Wegenbestand (NWB)"
)
__support_email__ = "beheerpdok@kadaster.nl"
__support_name__ = "PDOK Support"
__metadata_date__ = datetime.date(2026, 4, 18)

# Python module author (not PDOK/RWS)
__author__ = "Ruben Swarts"
__author_email__ = "aj.rubenswarts@gmail.com"
__author_github__ = "https://github.com/ajruben"
__developer_note__ = (
    "This Python module only; not affiliated with PDOK/RWS; does not maintain the API or dataset."
)

class NWBWegen:
    NWB_METADATA = APIMetadata(
        title=__title__,
        version=__api_version__,
        description=__description__,
        license=__license__,
        license_url=__license_url__,
        api_base_url=__url__,
        data_provider=__data_provider__,
        developer=__author__,
        developer_github=__author_github__,
        developer_note=__developer_note__,
        support_email=__support_email__,
        support_name=__support_name__,
        support_url=__api_support__,
        metadata_date=__metadata_date__,
    )
    _META_FIELD_NAMES = tuple(f.name for f in fields(APIMetadata))
    _META_KEYS = frozenset(_META_FIELD_NAMES)

    def __init__(self, pdok_client: Any) -> None:
        self.pdok_client = pdok_client
        self.pdok_session = pdok_client.pdok_session
        self.nwb_endpoint = urljoin(self.pdok_client.rws_url, "nationaal-wegenbestand-wegen/ogc/v1/")

    @property
    def metadata(self) -> APIMetadata:
        return type(self).NWB_METADATA

    # COMMON endpoints - always return as dict
    def get_landing_page(self) -> dict[str, Any]:
        """Calling root endpoint (The landing page) provides links to the API definition
        and the conformance statements for this API.

        Returns:
            dict[str, Any]
        """
        return self.pdok_client.get(
            self.nwb_endpoint, "", params=self.pdok_client.JSON_PARAM
        )

    def get_service_description(self) -> dict[str, Any]:
        "OpenAPI 3.0 via landing page service-desc link."
        landing = self.get_landing_page()
        return self.pdok_client.ogc_openapi_document(list(landing.get("links", [])))

    def get_conformance(self) -> dict[str, Any]:
        """NWB - Wegen - Conformance

        A list of all conformance classes specified in a standard that the server conforms to,
        under key 'conformTo'.

        Returns:
            dict[str, Any]
        """
        return self.pdok_client.ogc_conformance(
            self.nwb_endpoint, params=self.pdok_client.JSON_PARAM
        )

    # COLLECTIONS endpoints - always dict
    def get_collection(self, collection: str | None = None) -> dict[str, Any]:
        """GET ``/collections`` or ``/collections/{collection_id}`` (server defines ids)."""
        p = self.pdok_client.JSON_PARAM
        if collection is None:
            return self.pdok_client.ogc_collections(self.nwb_endpoint, params=p)
        return self.pdok_client.ogc_collection(self.nwb_endpoint, collection, params=p)

    # Features (actual geodata)
    def get_nwb_wegen_wegvakken(self) -> dict[str, Any]:
        """
        Retrieve the OGC API Features service of Nationaal Wegen Bestand (NWB) - wegen.
        Just the 'wegavkken' (roads), not hectometer paaltjes.

        source: https://api.pdok.nl/rws/nationaal-wegenbestand-wegen/ogc/v1/
        doc: https://api.pdok.nl/rws/nationaal-wegenbestand-wegen/ogc/v1/api?f=html
        """
        return self.pdok_client.ogc_collection(
            self.nwb_endpoint, "wegvakken", params=self.pdok_client.JSON_PARAM
        )
    
    def __getitem__(self, key: str) -> Any:
        cls = type(self)
        if key not in cls._META_KEYS:
            raise KeyError(key)
        return getattr(cls.NWB_METADATA, key)

    def __repr__(self) -> str:
        m = type(self).NWB_METADATA
        return (
            f"{self.__class__.__name__}("
            f"title={m.title!r}, provider={m.data_provider!r}, url={m.api_base_url!r})"
        )

    def __str__(self) -> str:
        m = type(self).NWB_METADATA
        return f"{m.title} — {m.data_provider} · v{m.version} · {m.api_base_url}"
