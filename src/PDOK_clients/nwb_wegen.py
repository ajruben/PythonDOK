"""python module to call OGC API Features service for Nationaal Wegen Bestand (NWB) - Wegen."""
from __future__ import annotations

import datetime
from dataclasses import fields
from typing import Any, Literal

from urllib.parse import urljoin

from .metadata import APIMetadata

# --- API + dataset metadata (dunders → NWB_METADATA) ---
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

# collection for this dataset
Collection= Literal["wegvakken", "hectopunten"]
COLLECTION_VALUES: frozenset[str] = frozenset({"wegvakken", "hectopunten"})

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
    # metadata of api, by api.
    def get_landing_page(self) -> dict[str, Any]:
        """Calling root endpoint (The landing page) provides links to the API definition 
        and the conformance statements for this API.

        Returns:
            dict[str, Any]
        """
        resp = self.pdok_session.get(
            self.nwb_endpoint,
            params={"f": "json"},
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()
    
    def get_service_description(self) -> dict[str, Any]:
        "The JSON OpenAPI 3.0 document that describes the API service offered at this endpoint"
        resp = self.pdok_session.get(
            urljoin(self.nwb_endpoint, "api"),
            params={"f": "json"},
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()
    
    def get_api_conformance(self) -> dict[str, Any]:
        """NWB - Wegen - Conformance

        A list of all conformance classes specified in a standard that the server conforms to,
        under key 'conformTo'.

        Returns:
            dict[str, Any]
        """
        resp = self.pdok_session.get(
            urljoin(self.nwb_endpoint, "conformance"),
            params={"f": "json"},
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()
        
    #COLLECTIONS endpoints - always dict
    def get_collection(
        self, collection: Collection | None = None
    ) -> dict[str, Any]:
        """ist collections or describe one of the dataset of NWB.

        Args:
            collection: 
                None (default) — GET .../collections (all collections).
                'wegvakken' or 'hectopunten' — GET .../collections/<value>.

        Raises:
            ValueError: If collection is not None and not in COLLECTION_VALUES.
        """
        if collection is not None and collection not in COLLECTION_VALUES:
            raise ValueError(
                f"collection must be None or one of {COLLECTION_VALUES}, got {collection}"
            )
        path = (
            "collections" if collection is None else f"collections/{collection}"
        )
        resp = self.pdok_session.get(
            urljoin(self.nwb_endpoint, path),
            params={"f": "json"},
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()
                
    #FEATURES schemas - always dict
    
    #Features (actual geodata)
    def get_nwb_wegen_wegvakken(self) -> dict[str, Any]:
        """
        Retrieve the OGC API Features service of Nationaal Wegen Bestand (NWB) - wegen.
        Just the 'wegavkken' (roads), not hectometer paaltjes.

        source: https://api.pdok.nl/rws/nationaal-wegenbestand-wegen/ogc/v1/
        doc: https://api.pdok.nl/rws/nationaal-wegenbestand-wegen/ogc/v1/api?f=html
        """
        pc = self.pdok_client
        nwb_api_wegvakken_endpoint = urljoin(
            pc.rws_url, "nationaal-wegenbestand-wegen/ogc/v1/collections/wegvakken"
        )
        resp = pc.pdok_session.get(
            nwb_api_wegvakken_endpoint,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()
    
    
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
