"""
https://www.pdok.nl/pdc-afnemers-van-data
"""

from __future__ import annotations

import logging
from collections import namedtuple
from typing import Any, ClassVar, Literal, cast, get_args
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter, Retry

from pyproj import CRS

from .nwb_wegen import NWBWegen

LOGGER = logging.getLogger(__name__)

# OGC API — Common / OpenAPI link types
_OPENAPI_JSON = "application/vnd.oai.openapi+json;version=3.0"
_OPENAPI_YAML = "application/vnd.oai.openapi;version=3.0"  # link type only; body still JSON from PDOK
_USER_AGENT = "pythonDOK (OGC API client)"

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

    #: Many PDOK OGC URLs accept ``f=json`` for JSON (pass as ``params=`` to :meth:`get` / ``ogc_*``).
    JSON_PARAM: ClassVar[dict[str, str]] = {"f": "json"}

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
        self.http_timeout = 30.0
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502],
        )
        self.pdok_session.mount("http://", HTTPAdapter(max_retries=retries))

        self.nwb_wegen = NWBWegen(pdok_client=self)

    # OGC API Common helpers (for dataset modules; all use pdok_session)
    def build_url(
        self,
        base_url: str,
        path: str = "",
        *,
        params: dict[str, Any] | None = None,
    ) -> str:
        """Resolve ``path`` under ``base_url`` and merge query strings via :mod:`urllib.parse`."""
        base = urlparse(base_url)
        root = urlunparse((base.scheme, base.netloc, base.path, "", "", ""))
        rel = (path or "").strip()
        if rel:
            join_base = root if root.endswith("/") else root + "/"
            resolved = urljoin(join_base, rel)
        else:
            resolved = root
        out = urlparse(resolved)

        pairs: list[tuple[str, str]] = []
        pairs.extend(parse_qsl(base.query, keep_blank_values=True))
        if params:
            pairs.extend(parse_qsl(urlencode(params, doseq=True), keep_blank_values=True))

        query = urlencode(pairs, doseq=True) if pairs else ""
        return urlunparse((out.scheme, out.netloc, out.path, out.params, query, out.fragment))

    def get(
        self,
        base_url: str,
        path: str = "",
        *,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """GET JSON from an OGC API — Common base URL (relative ``path`` under base)."""
        url = self.build_url(base_url, path, params=params)
        LOGGER.debug("OGC GET %s", url)
        headers: dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": _USER_AGENT,
        }
        if extra_headers:
            headers.update(extra_headers)
        r = self.pdok_session.get(url, headers=headers, timeout=self.http_timeout)
        r.raise_for_status()
        if not r.content:
            return {}
        return r.json()

    def ogc_openapi_document(self, links: list[dict[str, Any]]) -> dict[str, Any]:
        """OpenAPI 3.0 from landing_page.links (``rel=service-desc``); response parsed as JSON."""
        href: str | None = None
        for link in links:
            if link.get("rel") == "service-desc" and link.get("type") == _OPENAPI_JSON:
                href = link["href"]
                break

        if href is None:
            LOGGER.error("Did not find service-desc link")
            raise RuntimeError("Did not find service-desc link")
        LOGGER.debug("OpenAPI GET %s", href)
        r = self.pdok_session.get(
            href,
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": f"{_OPENAPI_JSON}, application/json",
            },
            timeout=self.http_timeout,
        )
        r.raise_for_status()
        return r.json()

    def ogc_conformance(
        self, base_url: str, *, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self.get(base_url, "conformance", params=params)

    def ogc_collections(
        self, base_url: str, *, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self.get(base_url, "collections", params=params)

    def ogc_collection(
        self,
        base_url: str,
        collection_id: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            return self.get(
                base_url, f"collections/{collection_id}", params=params
            )
        except requests.HTTPError as err:
            resp = err.response
            if resp is None or resp.status_code != 404:
                raise
            try:
                listing = self.ogc_collections(base_url, params=params)
            except requests.HTTPError:
                LOGGER.warning(
                    "Could not fetch /collections after 404 for %r", collection_id
                )
                raise err from None
            raw = listing.get("collections", [])
            available = sorted(
                {
                    str(c["id"])
                    for c in raw
                    if isinstance(c, dict) and c.get("id") is not None
                }
            )
            avail_msg = ", ".join(available) if available else "(none listed)"
            raise ValueError(
                f"OGC collection {collection_id!r} not found (404). "
                f"Available collection ids: {avail_msg}"
            ) from err

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
