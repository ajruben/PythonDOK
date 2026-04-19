"""Python module to call OGC API Features service for Nationaal Wegen Bestand (NWB) - Wegen."""
from __future__ import annotations

import datetime
import logging
from collections.abc import Sequence
from dataclasses import fields
from typing import Any, Literal

from urllib.parse import urljoin

import geopandas as gpd
from shapely.geometry.base import BaseGeometry
from tqdm.auto import tqdm
from pyproj import CRS

from .metadata import APIMetadata

LOGGER = logging.getLogger(__name__)

# OGC bbox query: 4-6 numbers per OpenAPI (4 = 2D extent, 6 = 3D); axis order follows bbox_crs.
BBox = Sequence[float]

# Polygon-like inputs accepted in place of bbox; their bounds drive the API call,
# their full geometry drives the post-fetch clip.
PolygonLike = BaseGeometry | gpd.GeoSeries | gpd.GeoDataFrame

# OpenAPI f-features on /items: output media (not the CRS query parameter).
FeaturesItemsFormat = Literal["json", "jsonfg", "html"]

# HTTP Accept header per f-features value, sent with /items requests for proper content negotiation.
ITEMS_ACCEPT_BY_F: dict[str, str] = {
    "json": "application/geo+json, application/json;q=0.5",
    "jsonfg": "application/vnd.ogc.fg+json, application/json;q=0.5",
    "html": "text/html",
}

# OpenAPI limit on /items: integer 1..1000; omit param to use server default (10).
ITEMS_LIMIT_MIN = 1
ITEMS_LIMIT_MAX = 1000
ITEMS_LIMIT_DEFAULT = 10

# --- API + dataset metadata ---
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
    """Client for the PDOK NWB-Wegen OGC API Features service.

    Wraps a PDOKClient instance and exposes typed helpers for landing page,
    conformance, collections, schemas, and the wegvakken /items endpoint.
    """

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
        """Initialize the NWB-Wegen client.

        Args:
            pdok_client: A PDOKClient instance providing the HTTP session and
                OGC API helpers used by this client.
        """
        self.pdok_client = pdok_client
        self.pdok_session = pdok_client.pdok_session
        self.nwb_endpoint = urljoin(self.pdok_client.rws_url, "nationaal-wegenbestand-wegen/ogc/v1/")

    @property
    def metadata(self) -> APIMetadata:
        """Return the static APIMetadata describing this dataset and module."""
        return type(self).NWB_METADATA

    # COMMON endpoints - always return as dict
    def get_landing_page(self) -> dict[str, Any]:
        """Get the OGC API landing page (root) for NWB-Wegen.

        Returns:
            dict[str, Any]: The landing-page document with links to the API
            definition and conformance statements.
        """
        return self.pdok_client.get(
            self.nwb_endpoint, "", params=self.pdok_client.JSON_PARAM
        )

    def get_service_description(self) -> dict[str, Any]:
        """Get the OpenAPI 3.0 service description.

        Returns:
            dict[str, Any]: OpenAPI document fetched via the landing page
            service-desc link.
        """
        landing = self.get_landing_page()
        return self.pdok_client.ogc_openapi_document(list(landing.get("links", [])))

    def get_conformance(self) -> dict[str, Any]:
        """Get the OGC API conformance declaration for NWB-Wegen.

        Returns:
            dict[str, Any]: Conformance document with a conformsTo list of all
            conformance class URIs the server claims to implement.
        """
        return self.pdok_client.ogc_conformance(
            self.nwb_endpoint, params=self.pdok_client.JSON_PARAM
        )

    # COLLECTIONS endpoints - always dict
    def get_collection(self, collection: str | None = None) -> dict[str, Any]:
        """Get the collections listing or a single collection by id.

        Args:
            collection: Collection id. If None, returns the full /collections
                listing; otherwise returns /collections/{collection}.

        Returns:
            dict[str, Any]: Either the collections listing or one collection
            description.
        """
        p = self.pdok_client.JSON_PARAM
        if collection is None:
            return self.pdok_client.ogc_collections(self.nwb_endpoint, params=p)
        return self.pdok_client.ogc_collection(self.nwb_endpoint, collection, params=p)

    # Schemas (OGC JSON Schema for feature properties + geometry)
    def get_wegvakken_schema(self) -> dict[str, Any]:
        """Get the JSON Schema for the wegvakken collection features.

        Returns:
            dict[str, Any]: JSON Schema document for /collections/wegvakken/schema.
        """
        return self.pdok_client.ogc_collection_schema(
            self.nwb_endpoint, "wegvakken", params=self.pdok_client.JSON_PARAM
        )

    def get_hectopunten_schema(self) -> dict[str, Any]:
        """Get the JSON Schema for the hectopunten collection features.

        Returns:
            dict[str, Any]: JSON Schema document for /collections/hectopunten/schema.
        """
        return self.pdok_client.ogc_collection_schema(
            self.nwb_endpoint, "hectopunten", params=self.pdok_client.JSON_PARAM
        )

    # get features
    def get_wegvakken(
        self,
        bbox: BBox | None = None,
        *,
        polygon: PolygonLike | None = None,
        validate_geojson: bool = True,
        bbox_crs: Any | None = None,
        crs: Any | None = None,
        limit: int = ITEMS_LIMIT_MAX,
        f: FeaturesItemsFormat = "json",
        progress: bool = False,
        gme_naam: str | None = None,
        stt_naam: str | None = None,
        wegbehsrt: str | None = None,
        wegnummer: str | None = None,
        wvk_id: float | int | None = None,
    ) -> gpd.GeoDataFrame:
        """Fetch wegvakken features within bbox or polygon as a single GeoDataFrame.

        Provide exactly one of bbox or polygon. With polygon, the OGC bbox
        query parameter is set to the polygon's bounds (rectangular candidate
        set from the server), then the merged frame is geometrically clipped
        to the polygon so the result matches the requested area exactly.
        Uses cursor pagination internally.

        Args:
            bbox: Sequence of 4 to 6 numbers (OpenAPI: array, minItems 4,
                maxItems 6) in the axis order of bbox_crs. Mutually exclusive
                with polygon.
            polygon: Shapely Polygon/MultiPolygon, GeoSeries or GeoDataFrame.
                Its bounds drive the API call, its full geometry clips the
                results. If a GeoSeries/GeoDataFrame supplies a CRS, that CRS
                is used for both bbox_crs (when not given) and the clip
                reprojection. Plain shapely geometries have no CRS, so
                bbox_crs must be set in that case (defaults to client crs_uri
                when omitted, same as bbox).
            validate_geojson: If True, validate each FeatureCollection page
                with FeatureCollectionModel before building the frame.
            bbox_crs: CRS for the bbox / polygon values. Accepted by pyproj
                CRS.from_user_input and must resolve to one of PDOK's allowed
                URIs. None uses the client's default crs_uri.
            crs: CRS for the response geometries. Same rules as bbox_crs.
            limit: Page size; integer in range(1, 1001).
            f: Output media (OpenAPI f-features). One of:

                - "json": plain GeoJSON FeatureCollection.
                - "jsonfg": OGC Features JSON-FG.
                - "html": HTML page; not usable with this GeoDataFrame path.

            progress: If True, show a tqdm bar over pages.
            gme_naam: Server-side filter on the gme_naam property
                (gemeentenaam).
            stt_naam: Server-side filter on the stt_naam property
                (straatnaam).
            wegbehsrt: Server-side filter on the wegbehsrt property
                (wegbeheerder soort).
            wegnummer: Server-side filter on the wegnummer property.
            wvk_id: Server-side filter on the wvk_id property (wegvak id).

        Returns:
            geopandas.GeoDataFrame: Merged features for all pages with the
            geometry column set to the resolved out_crs. When polygon is
            given, geometries are clipped to that polygon.

        Raises:
            ValueError: If neither or both of bbox and polygon are given, or
                if limit is not an int in [ITEMS_LIMIT_MIN, ITEMS_LIMIT_MAX].
            TypeError: If polygon is not Shapely geometry, GeoSeries or
                GeoDataFrame.
            requests.HTTPError: For non-2xx HTTP responses from the API.
        """
        if (bbox is None) == (polygon is None):
            raise ValueError("Provide exactly one of bbox or polygon")
        if type(limit) is not int or limit < ITEMS_LIMIT_MIN or limit > ITEMS_LIMIT_MAX:
            raise ValueError(
                f"limit must be int in [{ITEMS_LIMIT_MIN}, {ITEMS_LIMIT_MAX}], got {limit!r}"
            )

        pc = self.pdok_client
        clip_geom: BaseGeometry | None = None
        clip_crs: CRS | None = None
        if polygon is not None:
            raw_geom, raw_crs = pc.resolve_polygon(polygon)
            if raw_geom.is_empty:
                raise ValueError("polygon is empty")
            # bbox_crs precedence: explicit kwarg > polygon's own crs > client default.
            if bbox_crs is None and raw_crs is not None:
                bbox_crs = raw_crs
            request_crs = CRS(pc.resolve_query_crs(bbox_crs))
            if raw_crs is not None:
                src_crs = CRS.from_user_input(raw_crs)
                if not src_crs.equals(request_crs):
                    raw_geom = (
                        gpd.GeoSeries([raw_geom], crs=src_crs)
                        .to_crs(request_crs)
                        .iloc[0]
                    )
            clip_geom = raw_geom
            clip_crs = request_crs
            bbox = clip_geom.bounds  # in request_crs, matches bbox-crs query param

        merged: dict[str, Any] = {
            "f": f,
            "bbox": pc.bbox_to_query_param(bbox),
            "bbox-crs": pc.resolve_query_crs(bbox_crs),
            "crs": pc.resolve_query_crs(crs),
            "limit": limit,
        }
        if gme_naam is not None:
            merged["gme_naam"] = gme_naam
        if stt_naam is not None:
            merged["stt_naam"] = stt_naam
        if wegbehsrt is not None:
            merged["wegbehsrt"] = wegbehsrt
        if wegnummer is not None:
            merged["wegnummer"] = wegnummer
        if wvk_id is not None:
            merged["wvk_id"] = wvk_id

        out_crs = CRS(pc.resolve_query_crs(crs))
        LOGGER.info(
            "wegvakken: start paginated fetch bbox=%s limit=%s f=%s",
            merged["bbox"], limit, f,
        )

        accept = ITEMS_ACCEPT_BY_F[f]
        chunks: list[gpd.GeoDataFrame] = []
        next_url: str | None = None
        page_num = 0
        pbar = tqdm(
            desc="wegvakken",
            unit="page",
            disable=not progress,
            dynamic_ncols=True,
        )
        try:
            while True:
                page_num += 1
                if next_url is None:
                    fc = pc.ogc_collection_items(
                        self.nwb_endpoint,
                        "wegvakken",
                        params=merged,
                        extra_headers={"Accept": accept},
                    )
                else:
                    fc = pc.fetch_json(next_url, accept=accept)
                chunk = pc.feature_collection_to_geodataframe(
                    fc, crs=out_crs, validate=validate_geojson,
                )
                chunks.append(chunk)
                cum = sum(len(c) for c in chunks)
                LOGGER.info(
                    "wegvakken: page %d +%d features (%d cumulative)",
                    page_num, len(chunk), cum,
                )
                pbar.update(1)
                pbar.set_postfix_str(f"{cum} feats")
                next_url = pc.ogc_items_next_href(fc)
                if next_url is None:
                    break
        finally:
            pbar.close()

        if len(chunks) == 1:
            gdf = chunks[0]
        else:
            gdf = gpd.GeoDataFrame(gpd.pd.concat(chunks, ignore_index=True))

        if clip_geom is not None:
            mask = gpd.GeoSeries([clip_geom], crs=clip_crs)
            if not mask.crs.equals(out_crs):
                mask = mask.to_crs(out_crs)
            n_before = len(gdf)
            gdf = gpd.clip(gdf, mask)
            LOGGER.info(
                "wegvakken: clipped to polygon, %d -> %d features",
                n_before, len(gdf),
            )

        LOGGER.info(
            "wegvakken: done %d pages, %d features total",
            page_num,
            len(gdf),
        )
        return gdf

    # Features (actual geodata)
    def get_nwb_wegen_wegvakken(self) -> dict[str, Any]:
        """Get the wegvakken collection description (not features).

        Returns:
            dict[str, Any]: Collection description for /collections/wegvakken.

        See:
            source: https://api.pdok.nl/rws/nationaal-wegenbestand-wegen/ogc/v1/
            doc: https://api.pdok.nl/rws/nationaal-wegenbestand-wegen/ogc/v1/api?f=html
        """
        return self.pdok_client.ogc_collection(
            self.nwb_endpoint, "wegvakken", params=self.pdok_client.JSON_PARAM
        )

    def __getitem__(self, key: str) -> Any:
        """Read a metadata field by name.

        Args:
            key: Field name from APIMetadata.

        Returns:
            Any: Metadata value for key.

        Raises:
            KeyError: If key is not a known APIMetadata field.
        """
        cls = type(self)
        if key not in cls._META_KEYS:
            raise KeyError(key)
        return getattr(cls.NWB_METADATA, key)

    def __repr__(self) -> str:
        """Return a developer-oriented repr including title, provider and base URL."""
        m = type(self).NWB_METADATA
        return (
            f"{self.__class__.__name__}("
            f"title={m.title!r}, provider={m.data_provider!r}, url={m.api_base_url!r})"
        )

    def __str__(self) -> str:
        """Return a human-friendly summary string for this client."""
        m = type(self).NWB_METADATA
        return f"{m.title} - {m.data_provider} - v{m.version} - {m.api_base_url}"
