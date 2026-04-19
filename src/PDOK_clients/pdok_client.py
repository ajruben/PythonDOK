"""PDOK HTTP client and OGC API helpers.

See: https://www.pdok.nl/pdc-afnemers-van-data
"""
from __future__ import annotations

import logging
from collections import namedtuple
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, cast, get_args
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import geopandas as gpd
import requests
from pydantic_geojson import FeatureCollectionModel
from requests.adapters import HTTPAdapter, Retry
from shapely.geometry.base import BaseGeometry

from pyproj import CRS

from .nwb_wegen import NWBWegen

LOGGER = logging.getLogger(__name__)

# OGC API - Common / OpenAPI link types
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

# OGC API Features bbox query (OpenAPI): type=array, items=number, minItems=4, maxItems=6
_BBOX_MIN_ITEMS = 4
_BBOX_MAX_ITEMS = 6


class PDOKClient:
    """HTTP client for PDOK APIs with OGC API - Features helpers.

    The class wires a requests Session with retry, exposes URL/query helpers,
    OGC API - Common method shortcuts, and small GeoJSON utilities. Per-dataset
    clients (e.g. NWBWegen) reuse this client through composition.

    Attributes:
        crs: pyproj CRS used as the default for query CRS resolution.
        crs_uri: PDOK URI form of the default CRS.
        api_pdok_url: Base URL https://api.pdok.nl/.
        rws_url: Base URL for RWS-hosted services under api.pdok.nl.
        pdok_session: Configured requests.Session with retry mounted.
        http_timeout: Per-request timeout in seconds.
        nwb_wegen: NWBWegen client wired to this PDOKClient.
    """

    #: Many PDOK OGC URLs accept f=json for JSON. Pass as params to get / ogc_*.
    JSON_PARAM: ClassVar[dict[str, str]] = {"f": "json"}

    Components = namedtuple(
        typename="Components",
        field_names=["scheme", "netloc", "url", "path", "query", "fragment"],
    )

    def __init__(self, crs: Any | None = None) -> None:
        """Initialize the client and resolve the default CRS.

        Args:
            crs: CRS understood by pyproj.CRS.from_user_input. Must match one of
                the PDOK-allowed CRS definitions (any equivalent input form
                works). Allowed targets:

                - OGC CRS84 - http://www.opengis.net/def/crs/OGC/1.3/CRS84
                - EPSG:28992 - Amersfoort / RD New
                - EPSG:3857  - WGS 84 / Pseudo-Mercator
                - EPSG:4258  - ETRS89

                If None, OGC CRS84 (WGS 84 longitude/latitude) is used.

        Raises:
            ValueError: If crs is not equivalent to one of the allowed CRSs.
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

    @staticmethod
    def bbox_to_query_param(bbox: Sequence[float]) -> str:
        """Format an OGC bbox query value as comma-separated numbers.

        Matches the OpenAPI fragment: type=array, items=number, minItems=4,
        maxItems=6.

        Args:
            bbox: Sequence of 4 to 6 numbers.

        Returns:
            str: Comma-separated stringified floats.

        Raises:
            ValueError: If len(bbox) is not in [4, 6].
        """
        n = len(bbox)
        if not _BBOX_MIN_ITEMS <= n <= _BBOX_MAX_ITEMS:
            raise ValueError(
                f"bbox length must be between {_BBOX_MIN_ITEMS} and {_BBOX_MAX_ITEMS} "
                "(OpenAPI: type array, items number, minItems 4, maxItems 6)."
            )
        return ",".join(str(float(x)) for x in bbox)

    # OGC API Common helpers (for dataset modules; all use pdok_session)
    def build_url(
        self,
        base_url: str,
        path: str = "",
        *,
        params: dict[str, Any] | None = None,
    ) -> str:
        """Resolve path under base_url and merge query strings via urllib.parse.

        Args:
            base_url: Absolute URL acting as the base; existing query string is
                preserved.
            path: Relative path under base_url. Empty keeps base path.
            params: Query parameters to merge in addition to those already on
                base_url. Supports list values via doseq.

        Returns:
            str: Final URL with merged query string.
        """
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
        """GET JSON from an OGC API base URL with relative path.

        Args:
            base_url: Absolute base URL of the OGC API service.
            path: Relative path under base_url.
            params: Query parameters merged into the URL.
            extra_headers: Optional headers added on top of the defaults.

        Returns:
            dict[str, Any]: Parsed JSON response, or {} if the body is empty.

        Raises:
            requests.HTTPError: For non-2xx HTTP responses.
        """
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

    def fetch_json(self, url: str, *, accept: str = "application/json") -> dict[str, Any]:
        """GET JSON from a fully formed URL.

        Useful for following pagination links such as the result of
        ogc_items_next_href.

        Args:
            url: Absolute URL to fetch.
            accept: HTTP Accept header value. Defaults to application/json;
                pass application/geo+json or application/vnd.ogc.fg+json when
                following next links from an /items response.

        Returns:
            dict[str, Any]: Parsed JSON response, or {} if the body is empty.

        Raises:
            requests.HTTPError: For non-2xx HTTP responses.
        """
        LOGGER.debug("GET %s", url)
        headers: dict[str, str] = {
            "Accept": accept,
            "User-Agent": _USER_AGENT,
        }
        r = self.pdok_session.get(url, headers=headers, timeout=self.http_timeout)
        r.raise_for_status()
        if not r.content:
            return {}
        return r.json()

    def ogc_openapi_document(self, links: list[dict[str, Any]]) -> dict[str, Any]:
        """Fetch the OpenAPI 3.0 document referenced from landing-page links.

        Looks for the link with rel=service-desc and the OpenAPI JSON type.

        Args:
            links: List of link objects from the landing page.

        Returns:
            dict[str, Any]: Parsed OpenAPI document.

        Raises:
            RuntimeError: If no service-desc link with the expected JSON type
                is found.
            requests.HTTPError: For non-2xx HTTP responses.
        """
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
        """GET /conformance for an OGC API service.

        Args:
            base_url: Base URL of the OGC API service.
            params: Query parameters to attach.

        Returns:
            dict[str, Any]: Conformance document (conformsTo array).
        """
        return self.get(base_url, "conformance", params=params)

    def ogc_collections(
        self, base_url: str, *, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """GET /collections for an OGC API service.

        Args:
            base_url: Base URL of the OGC API service.
            params: Query parameters to attach.

        Returns:
            dict[str, Any]: Collections listing.
        """
        return self.get(base_url, "collections", params=params)

    def ogc_collection(
        self,
        base_url: str,
        collection_id: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """GET /collections/{collection_id} with helpful 404 handling.

        On HTTP 404, fetches the collections listing and raises ValueError with
        the available collection ids.

        Args:
            base_url: Base URL of the OGC API service.
            collection_id: Target collection id.
            params: Query parameters to attach.

        Returns:
            dict[str, Any]: Collection description.

        Raises:
            ValueError: If the collection is missing (404) and the listing
                could be retrieved.
            requests.HTTPError: For other non-2xx HTTP responses, or when the
                fallback listing also fails.
        """
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

    def ogc_collection_schema(
        self,
        base_url: str,
        collection_id: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """GET /collections/{collection_id}/schema (JSON Schema for features).

        Args:
            base_url: Base URL of the OGC API service.
            collection_id: Target collection id.
            params: Query parameters to attach.

        Returns:
            dict[str, Any]: JSON Schema document.
        """
        return self.get(
            base_url, f"collections/{collection_id}/schema", params=params
        )

    def ogc_collection_items(
        self,
        base_url: str,
        collection_id: str,
        *,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """GET /collections/{collection_id}/items as JSON.

        Often a GeoJSON FeatureCollection when f=json (or JSON-FG when f=jsonfg).

        Args:
            base_url: Base URL of the OGC API service.
            collection_id: Target collection id.
            params: Query parameters to attach (e.g. bbox, limit, f, crs).
            extra_headers: Optional extra HTTP headers, typically used to set
                an Accept value matching the f query parameter.

        Returns:
            dict[str, Any]: Parsed FeatureCollection (or equivalent) document.
        """
        return self.get(
            base_url,
            f"collections/{collection_id}/items",
            params=params,
            extra_headers=extra_headers,
        )

    def resolve_query_crs(self, crs: Any | None) -> str:
        """Resolve a CRS input to a PDOK-allowed crs URI for query parameters.

        Args:
            crs: Anything pyproj.CRS.from_user_input accepts. None returns
                self.crs_uri (the client's default).

        Returns:
            str: One of the PDOK-allowed crs URIs.

        Raises:
            ValueError: If crs is not equivalent to any allowed CRS.
        """
        if crs is None:
            return self.crs_uri
        return self._resolve_crs_uri(CRS.from_user_input(crs))

    # GeoJSON / OGC FeatureCollection helpers
    @staticmethod
    def ogc_items_next_href(items_fc: dict[str, Any]) -> str | None:
        """Return href of the links entry whose rel is 'next', or None.

        Args:
            items_fc: A FeatureCollection-like dict with a links list.

        Returns:
            str | None: The URL string for the next page, or None when no
            valid next link is present.
        """
        for link in items_fc.get("links") or []:
            if isinstance(link, dict) and link.get("rel") == "next":
                href = link.get("href")
                if isinstance(href, str) and href:
                    return href
        return None

    @staticmethod
    def json_schema_property_field_names(schema: dict[str, Any]) -> list[str]:
        """Return top-level properties keys from an OGC schema document.

        Args:
            schema: JSON Schema dict from /collections/{id}/schema.

        Returns:
            list[str]: Property names declared at the top level.
        """
        return list(schema.get("properties", {}).keys())

    @staticmethod
    def resolve_polygon(polygon: Any) -> tuple[BaseGeometry, Any | None]:
        """Extract a single Shapely geometry plus optional CRS from a polygon-like input.

        Useful for callers that want to drive an OGC items request from a
        polygon: take its bounds for the bbox query parameter and reuse the
        full geometry to clip the response.

        Args:
            polygon: Shapely geometry, GeoSeries, or GeoDataFrame.

        Returns:
            tuple: (geometry, crs). geometry is the union of all geometries
            when a multi-row GeoSeries/GeoDataFrame is passed. crs is None
            when the input has no CRS information (e.g. a bare Shapely
            geometry).

        Raises:
            TypeError: If polygon is not a supported type.
        """
        if isinstance(polygon, gpd.GeoDataFrame):
            return polygon.geometry.union_all(), polygon.crs
        if isinstance(polygon, gpd.GeoSeries):
            return polygon.union_all(), polygon.crs
        if isinstance(polygon, BaseGeometry):
            return polygon, None
        raise TypeError(
            "polygon must be a Shapely geometry, GeoSeries or GeoDataFrame; "
            f"got {type(polygon).__name__}"
        )

    @staticmethod
    def feature_collection_to_geodataframe(
        data: dict[str, Any],
        *,
        crs: Any | None = None,
        validate: bool = True,
    ) -> gpd.GeoDataFrame:
        """Build a GeoDataFrame from a GeoJSON FeatureCollection dict.




        Args:
            data: GeoJSON FeatureCollection dict.
            crs: CRS to attach to the resulting frame. Anything GeoPandas
                accepts; None leaves the geometry CRS unset.
            validate: If True (default), validate data with
                FeatureCollectionModel when crs is unknown or geographic.

        Returns:
            geopandas.GeoDataFrame: Frame with one row per feature.
        """
        if validate:
            crs_obj = CRS.from_user_input(crs) if crs is not None else None
            if crs_obj is None or crs_obj.is_geographic:
                FeatureCollectionModel.model_validate(data)
            else:
                LOGGER.debug(
                    "feature_collection_to_geodataframe: skip validation; "
                    "crs %s is projected (pydantic_geojson assumes lon/lat).",
                    crs_obj.to_string(),
                )
        return gpd.GeoDataFrame.from_features(data["features"], crs=crs)

    @staticmethod
    def _resolve_crs_uri(crs: CRS) -> CRSUri:
        """Map a pyproj CRS to one of PDOK's allowed crs URIs.

        Args:
            crs: pyproj.CRS to match against the allowed list.

        Returns:
            CRSUri: Matched PDOK URI string.

        Raises:
            ValueError: If crs is not equivalent to any allowed CRS.
        """
        for uri, ref in zip(_CRS_URIS, _ALLOWED_CRS, strict=True):
            if crs.equals(ref):
                return cast(CRSUri, uri)
        raise ValueError(
            "To use the PDOK API, the crs must be equivalent to one of the following "
            "supported CRS definitions in a format as accepted by pyproj. "
            f"{tuple(_CRS_URIS)}; got {crs}. Note: format does not matter as long as pyproj supports it."
        )
