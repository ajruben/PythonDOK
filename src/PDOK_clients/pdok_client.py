from collections import namedtuple
from typing import Any, Literal, cast, get_args

from urllib.parse import urljoin, urlunparse
import requests
from requests.adapters import HTTPAdapter, Retry

from pyproj import CRS

from .nwb_wegen import NWBWegen

# Allowed CRS for PDOK OGC Features (used in bbox-crs / crs params)
DEFAULT_CRS_URI = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
CRSUri = Literal[
    "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
    "http://www.opengis.net/def/crs/EPSG/0/28992",
    "http://www.opengis.net/def/crs/EPSG/0/3857",
    "http://www.opengis.net/def/crs/EPSG/0/4258",
]
_CRS_URIS: tuple[str, ...] = get_args(CRSUri)
_ALLOWED_CRS: tuple[CRS, ...] = tuple(CRS(u) for u in _CRS_URIS)


class PDOKClient:
    """HTTP client for PDOK APIs. crs is passed to pyproj CRS.from_user_input."""

    Components = namedtuple(
        typename="Components",
        field_names=["scheme", "netloc", "url", "path", "query", "fragment"],
    )

    def __init__(self, crs: Any | None = None) -> None:
        """
        Parameters
        ----------
        crs : optional
            If omitted, uses OGC CRS84 (WGS 84 longitude-latitude).

            passed value needs to be accepted by pyproj CRS.from_user_input
                        
            The CRS must match one of these definitions (any equivalent form works):

            - OGC CRS84
              http://www.opengis.net/def/crs/OGC/1.3/CRS84
              
            - EPSG:28992
              Amersfoort / RD New
              http://www.opengis.net/def/crs/EPSG/0/28992
              
            - EPSG:3857
              WGS 84 / Pseudo-Mercator
              http://www.opengis.net/def/crs/EPSG/0/3857
              
            - EPSG:4258
              ETRS89
              http://www.opengis.net/def/crs/EPSG/0/4258
        """
        if crs is None:
            self.crs = CRS(DEFAULT_CRS_URI)
            self.crs_uri = cast(CRSUri, DEFAULT_CRS_URI)
        else:
            crs = CRS.from_user_input(crs)
            self.crs_uri = self._resolve_crs_uri(crs)
            self.crs = crs

        self.api_pdok_url = urlunparse(
            self.Components(
                scheme="https",
                netloc="api.pdok.nl",
                query="",
                path="",
                url="/",
                fragment="",
            )
        )
        self.rws_url = urljoin(self.api_pdok_url, "rws/")
        self.pdok_session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502],
        )
        self.pdok_session.mount("http://", HTTPAdapter(max_retries=retries))

        self.nwb_wegen = NWBWegen(pdok_client=self)

    @staticmethod
    def _resolve_crs_uri(crs: CRS) -> CRSUri:
        """Return crs_uri for PDOK API params if crs matches an allowed definition."""
        for uri, ref in zip(_CRS_URIS, _ALLOWED_CRS, strict=True):
            if crs.equals(ref):
                return cast(CRSUri, uri)
        raise ValueError(
            "To use the PDOK API, the crs must be equivalent to one of the following supported CRS definitions in a format as accepted by pyproj."
            f"{tuple(_CRS_URIS)}; got {crs}. Note; format doesn't matter as long as pyproj supports it."
        )
