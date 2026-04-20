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

# OGC bbox query input: 4-6 numbers per OpenAPI (4 = 2D extent, 6 = 3D), or a
# geometry-like object (Shapely, GeoSeries, GeoDataFrame) whose bounds will be
# used. Axis order follows bbox_crs.
BBox = Sequence[float] | BaseGeometry | gpd.GeoSeries | gpd.GeoDataFrame

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

# --- API + dataset providers metadata (not module author)---
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

# Python module author (not PDOK/RWS affeliated)
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
        # general params
        bbox: BBox | None = None,             # only if polygon not provided
        polygon: PolygonLike | None = None,   # only if bbox not provided (queries bbox of polygon, response is clipped to polygon)
        validate_geojson: bool = True,
        bbox_crs: Any | None = None,
        crs: Any | None = None,
        limit: int = ITEMS_LIMIT_MAX,
        f: FeaturesItemsFormat = "json",
        progress: bool = False,
        # params specific to wegvakken/roads
        municipality_name: str | None = None,       # = gme_naam param
        street_name: str | None = None,             # = stt_naam
        road_manager: str | None = None,            # = wegbehsrt
        road_number: str | None = None,             # = wegnummer
        road_part_id: float | int | None = None,    # = wvk_id
    ) -> gpd.GeoDataFrame:
        """Fetch road segments ("wegvakken") for an area, returned as a GeoDataFrame.

        The server returns results in pages; this method walks through every
        page and stitches them into one GeoDataFrame for you.
        
        Pick one of two ways to say where you want roads from:
          - bbox: a rectangle as four numbers (minx, miny, maxx, maxy).
          - polygon: any format (Shapely Polygon/MultiPolygon, GeoSeries or
            GeoDataFrame). Its bbox is queried, the repsonse is clipped to polygon.

        Args:
            bbox: Four (or six, for 3D) numbers defining a rectangle, in the
                same axis order as bbox_crs. Use this OR polygon, not both.
            polygon: Any area (Shapely geometry, GeoSeries, or GeoDataFrame).
                If it carries a CRS, that CRS is used; otherwise the default
                (OGC:CRS84 longitude/latitude) is assumed.
            validate_geojson: Sanity-check each response page before parsing.
                Safe to leave on; automatically skipped for projected CRS.
            bbox_crs: CRS of the bbox/polygon coordinates. Defaults to the
                client default (OGC:CRS84). Must be one of PDOK's accepted
                CRSs: OGC:CRS84, EPSG:28992 (RD New), EPSG:3857, EPSG:4258.
            crs: CRS of the geometries returned in the GeoDataFrame.
                Same options as bbox_crs. EPSG:28992 is a good pick for
                measurements in meters across the Netherlands.
            limit: How many roads to request per page (1..1000). Bigger =
                fewer HTTP calls. Default 1000.
            f: Response format from the server.
                - "json": plain GeoJSON (default, recommended).
                - "jsonfg": OGC Features JSON-FG (extra metadata).
                - "html": HTML page (not usable here).
            progress: Show a tqdm progress bar across pages.

            Road attribute filters (all optional, applied server-side):

            municipality_name: Municipality (gemeente). Example: "Goeree-Overflakkee".
            street_name: Street (straatnaam). Example: "Stoofweg".
            road_manager: Road manager type (wegbeheerder soort). One of:

                - "G" : gemeente (municipality)
                - "P" : provincie
                - "R" : rijk (national)
                - "W" : waterschap
                - "T" : other (e.g. Staatsbosbeheer)

            road_number: Road number. Example: "057" (matches N57 / RW57).
            road_part_id: Exact road-segment id. Example: 600944149.

        Returns:
            geopandas.GeoDataFrame: All matching road segments, already
            merged across pages. When you pass a polygon, geometries are
            clipped to it so the result matches your area exactly.

        Raises:
            ValueError: If neither or both of bbox and polygon are given, or
                if limit is outside 1..1000.
            TypeError: If polygon is not a Shapely geometry, GeoSeries, or
                GeoDataFrame.
            requests.HTTPError: For non-2xx HTTP responses from the API.

        Examples:
            Fetch by bounding box:

            >>> gdf = nwb.get_wegvakken(bbox=(3.82, 51.62, 4.41, 51.86))

            Fetch for a polygon and clip to it, loaded from a GeoJSON file (but format is arbitrary):

            >>> aoi = gpd.read_file("aoi.geojson").set_crs("OGC:CRS84", allow_override=True)
            >>> gdf = nwb.get_wegvakken(polygon=aoi, progress=True)

            Same thing, but building the polygon directly with Shapely
            (lon/lat coordinates, so tag it as OGC:CRS84 via a GeoSeries):

            >>> from shapely.geometry import Polygon
            >>> poly = Polygon([
            ...     (3.82, 51.62), (4.00, 51.60), (4.25, 51.63), (4.41, 51.70),
            ...     (4.41, 51.80), (4.30, 51.86), (4.05, 51.86), (3.85, 51.82),
            ...     (3.82, 51.72), (3.82, 51.62),
            ... ])
            >>> aoi = gpd.GeoSeries([poly], crs="OGC:CRS84") # area of interest
            >>> gdf = nwb.get_wegvakken(polygon=aoi)

            Only roads managed by a provincie (province), on road N57:

            >>> gdf = nwb.get_wegvakken(
            ...     polygon=aoi, road_manager="P", road_number="057",
            ... )
        """
        if (bbox is None) == (polygon is None):
            raise ValueError("Provide exactly one of bbox or polygon")
        if type(limit) is not int or limit < ITEMS_LIMIT_MIN or limit > ITEMS_LIMIT_MAX:
            raise ValueError(
                f"limit must be int in [{ITEMS_LIMIT_MIN}, {ITEMS_LIMIT_MAX}], got {limit!r}"
            )

        # Single CRS for both the query and the clip: align to the response CRS.
        out_crs_uri = self.pdok_client.resolve_query_crs(crs)
        clip_geom: BaseGeometry | None = None

        if polygon is not None:
            if bbox_crs is not None:
                LOGGER.debug(
                    "polygon given; ignoring bbox_crs=%r (using response crs %r).",
                    bbox_crs, out_crs_uri,
                )
            clip_geom, out_crs_uri = self.pdok_client.prepare_polygon(
                polygon, target_crs=crs,
            )
            bbox = clip_geom.bounds  # already in out_crs_uri

        merged: dict[str, Any] = {
            "f": f,
            "bbox": self.pdok_client.bbox_to_query_param(bbox),
            "bbox-crs": out_crs_uri if polygon is not None else self.pdok_client.resolve_query_crs(bbox_crs),
            "crs": out_crs_uri,
            "limit": limit,
        }
        if municipality_name is not None:
            merged["gme_naam"] = municipality_name
        if street_name is not None:
            merged["stt_naam"] = street_name
        if road_manager is not None:
            merged["wegbehsrt"] = road_manager
        if road_number is not None:
            merged["wegnummer"] = road_number
        if road_part_id is not None:
            merged["wvk_id"] = road_part_id

        out_crs = CRS(out_crs_uri)
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
                    fc = self.pdok_client.ogc_collection_items(
                        self.nwb_endpoint,
                        "wegvakken",
                        params=merged,
                        extra_headers={"Accept": accept},
                    )
                else:
                    fc = self.pdok_client.fetch_json(next_url, accept=accept)
                chunk = self.pdok_client.feature_collection_to_geodataframe(
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
                next_url = self.pdok_client.ogc_items_next_href(fc)
                if next_url is None:
                    break
        finally:
            pbar.close()

        if len(chunks) == 1:
            gdf = chunks[0]
        else:
            gdf = gpd.GeoDataFrame(gpd.pd.concat(chunks, ignore_index=True))

        if clip_geom is not None:
            n_before = len(gdf)
            gdf = gpd.clip(gdf, gpd.GeoSeries([clip_geom], crs=out_crs))
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
