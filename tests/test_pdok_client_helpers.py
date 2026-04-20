"""Tests for PDOKClient query helpers: resolve_query_crs, prepare_polygon,
bbox_to_query_param."""
from __future__ import annotations

import logging

import geopandas as gpd
import pytest
from pyproj import CRS
from pyproj.exceptions import CRSError
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry

from src.PDOK_clients.pdok_client import DEFAULT_CRS_URI, PDOKClient


# --- fixtures -----------------------------------------------------------------
@pytest.fixture
def default_client() -> PDOKClient:
    """Client with default CRS (OGC:CRS84)."""
    return PDOKClient()


@pytest.fixture
def rd_client() -> PDOKClient:
    """Client whose default CRS is EPSG:28992 (RD New)."""
    return PDOKClient(crs="EPSG:28992")


@pytest.fixture
def sample_polygon() -> Polygon:
    """Simple CRS84 triangle around Amsterdam."""
    return Polygon([(4.89, 52.37), (4.92, 52.37), (4.89, 52.39)])


# --- resolve_query_crs --------------------------------------------------------
class TestResolveQueryCrs:
    def test_none_returns_client_default(self, default_client: PDOKClient) -> None:
        assert default_client.resolve_query_crs(None) == DEFAULT_CRS_URI

    def test_none_with_non_default_client(self, rd_client: PDOKClient) -> None:
        assert rd_client.resolve_query_crs(None) == (
            "http://www.opengis.net/def/crs/EPSG/0/28992"
        )

    @pytest.mark.parametrize(
        ("input_crs", "expected_uri"),
        [
            ("OGC:CRS84", DEFAULT_CRS_URI),
            ("EPSG:28992", "http://www.opengis.net/def/crs/EPSG/0/28992"),
            (28992, "http://www.opengis.net/def/crs/EPSG/0/28992"),
            (CRS("EPSG:3857"), "http://www.opengis.net/def/crs/EPSG/0/3857"),
            ("EPSG:4258", "http://www.opengis.net/def/crs/EPSG/0/4258"),
        ],
        ids=["CRS84-str", "EPSG:28992-str", "EPSG:28992-int", "CRS-obj-3857", "EPSG:4258-str"],
    )
    def test_allowed_values(
        self,
        default_client: PDOKClient,
        input_crs: object,
        expected_uri: str,
    ) -> None:
        assert default_client.resolve_query_crs(input_crs) == expected_uri

    def test_disallowed_crs_raises(self, default_client: PDOKClient) -> None:
        # EPSG:4326 has different axis-order vs CRS84; PDOK rejects it.
        with pytest.raises(ValueError, match="supported CRS"):
            default_client.resolve_query_crs("EPSG:4326")

    def test_unknown_crs_string_raises_crserror(
        self, default_client: PDOKClient
    ) -> None:
        with pytest.raises(CRSError):
            default_client.resolve_query_crs("nonsense")


# --- bbox_to_query_param ------------------------------------------------------
class TestBboxToQueryParam:
    def test_four_numbers(self) -> None:
        assert (
            PDOKClient.bbox_to_query_param((1, 2, 3, 4))
            == "1.0,2.0,3.0,4.0"
        )

    def test_six_numbers(self) -> None:
        assert (
            PDOKClient.bbox_to_query_param((1, 2, 3, 4, 5, 6))
            == "1.0,2.0,3.0,4.0,5.0,6.0"
        )

    def test_accepts_list(self) -> None:
        assert PDOKClient.bbox_to_query_param([1.5, 2.5, 3.5, 4.5]) == "1.5,2.5,3.5,4.5"

    def test_coerces_ints_to_floats(self) -> None:
        out = PDOKClient.bbox_to_query_param((1, 2, 3, 4))
        assert all(part.endswith(".0") for part in out.split(","))

    @pytest.mark.parametrize("bad_len", [0, 1, 2, 3, 7, 10])
    def test_bad_length_raises(self, bad_len: int) -> None:
        bbox = tuple(float(i) for i in range(bad_len))
        with pytest.raises(ValueError, match="bbox length"):
            PDOKClient.bbox_to_query_param(bbox)

    def test_non_numeric_raises(self) -> None:
        with pytest.raises((TypeError, ValueError)):
            PDOKClient.bbox_to_query_param(("a", "b", "c", "d"))

    # Geometry-like inputs: bounds are extracted automatically.
    def test_shapely_polygon_uses_bounds(self, sample_polygon: Polygon) -> None:
        out = PDOKClient.bbox_to_query_param(sample_polygon)
        minx, miny, maxx, maxy = sample_polygon.bounds
        assert out == f"{float(minx)},{float(miny)},{float(maxx)},{float(maxy)}"

    def test_shapely_point_uses_bounds(self) -> None:
        # Point has zero-area bounds (minx == maxx). Low-level formatter
        # accepts any geometry with bounds; polygon-only enforcement lives in
        # resolve_polygon.
        out = PDOKClient.bbox_to_query_param(Point(4.9, 52.38))
        assert out == "4.9,52.38,4.9,52.38"

    def test_geoseries_uses_total_bounds(
        self, sample_polygon: Polygon
    ) -> None:
        gs = gpd.GeoSeries([sample_polygon], crs="OGC:CRS84")
        minx, miny, maxx, maxy = sample_polygon.bounds
        assert PDOKClient.bbox_to_query_param(gs) == (
            f"{float(minx)},{float(miny)},{float(maxx)},{float(maxy)}"
        )

    def test_geodataframe_total_bounds_of_union(self) -> None:
        poly_a = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        poly_b = Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])
        gdf = gpd.GeoDataFrame(geometry=[poly_a, poly_b], crs="OGC:CRS84")
        assert PDOKClient.bbox_to_query_param(gdf) == "0.0,0.0,3.0,1.0"

    def test_empty_geometry_raises(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            PDOKClient.bbox_to_query_param(Polygon())

    def test_empty_geoseries_raises(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            PDOKClient.bbox_to_query_param(
                gpd.GeoSeries([], crs="OGC:CRS84")
            )

    def test_unsupported_type_raises(self) -> None:
        with pytest.raises(TypeError, match="bbox must be"):
            PDOKClient.bbox_to_query_param("1,2,3,4")  # type: ignore[arg-type]


# --- prepare_polygon ----------------------------------------------------------
class TestPreparePolygon:
    def test_geoseries_with_matching_target_no_reproject(
        self,
        default_client: PDOKClient,
        sample_polygon: Polygon,
    ) -> None:
        gs = gpd.GeoSeries([sample_polygon], crs="OGC:CRS84")
        geom, uri = default_client.prepare_polygon(gs, target_crs="OGC:CRS84")
        assert uri == DEFAULT_CRS_URI
        # identical coordinates (no reprojection happened)
        assert geom.equals_exact(sample_polygon, tolerance=1e-9)

    def test_geoseries_reprojects_to_target(
        self,
        default_client: PDOKClient,
        sample_polygon: Polygon,
    ) -> None:
        gs = gpd.GeoSeries([sample_polygon], crs="OGC:CRS84")
        geom, uri = default_client.prepare_polygon(gs, target_crs="EPSG:28992")
        assert uri == "http://www.opengis.net/def/crs/EPSG/0/28992"
        minx, miny, maxx, maxy = geom.bounds
        assert 50_000 < minx < 300_000
        assert 400_000 < miny < 600_000

    def test_geodataframe_input(
        self,
        default_client: PDOKClient,
        sample_polygon: Polygon,
    ) -> None:
        gdf = gpd.GeoDataFrame(geometry=[sample_polygon], crs="OGC:CRS84")
        geom, uri = default_client.prepare_polygon(gdf, target_crs="OGC:CRS84")
        assert uri == DEFAULT_CRS_URI
        assert geom.equals_exact(sample_polygon, tolerance=1e-9)

    def test_geodataframe_multi_row_unions(
        self, default_client: PDOKClient
    ) -> None:
        poly_a = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        poly_b = Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])
        gdf = gpd.GeoDataFrame(geometry=[poly_a, poly_b], crs="OGC:CRS84")
        geom, _ = default_client.prepare_polygon(gdf, target_crs="OGC:CRS84")
        assert geom.area == pytest.approx(poly_a.area + poly_b.area)

    def test_raw_shapely_no_crs_uses_target(
        self,
        default_client: PDOKClient,
        sample_polygon: Polygon,
    ) -> None:
        # Raw Shapely has no CRS; we trust the caller-provided target_crs.
        geom, uri = default_client.prepare_polygon(sample_polygon, target_crs="OGC:CRS84")
        assert uri == DEFAULT_CRS_URI
        assert geom.equals_exact(sample_polygon, tolerance=1e-9)

    def test_target_crs_none_inherits_from_polygon(
        self,
        default_client: PDOKClient,
        sample_polygon: Polygon,
    ) -> None:
        gs = gpd.GeoSeries([sample_polygon], crs="OGC:CRS84").to_crs(28992)
        geom, uri = default_client.prepare_polygon(gs, target_crs=None)
        # Inherited CRS (EPSG:28992) is PDOK-allowed, so it wins.
        assert uri == "http://www.opengis.net/def/crs/EPSG/0/28992"
        # No reprojection back to CRS84 -> RD New coords.
        assert geom.equals_exact(gs.iloc[0], tolerance=1e-6)

    def test_target_crs_none_and_raw_shapely_uses_client_default(
        self,
        rd_client: PDOKClient,
        sample_polygon: Polygon,
    ) -> None:
        # Raw Shapely has no CRS and target_crs is None -> client default wins.
        geom, uri = rd_client.prepare_polygon(sample_polygon, target_crs=None)
        assert uri == "http://www.opengis.net/def/crs/EPSG/0/28992"
        # No reprojection happens (shapely has no CRS, caller promises RD New).
        assert geom.equals_exact(sample_polygon, tolerance=1e-9)

    def test_inherited_disallowed_crs_warns_and_falls_back(
        self,
        default_client: PDOKClient,
        sample_polygon: Polygon,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # EPSG:4326 is not in PDOK's allow-list; with target_crs=None and the
        # polygon's crs inherited, we expect a warning and a fall-back to the
        # client default.
        gs = gpd.GeoSeries([sample_polygon], crs=4326)
        with caplog.at_level(logging.WARNING, logger="src.PDOK_clients.pdok_client"):
            geom, uri = default_client.prepare_polygon(gs, target_crs=None)
        assert uri == DEFAULT_CRS_URI
        assert any("not accepted by PDOK" in r.message for r in caplog.records)
        # Polygon should be reprojected to CRS84 (same numeric values here).
        assert geom.equals_exact(sample_polygon, tolerance=1e-6)

    def test_explicit_disallowed_target_raises(
        self,
        default_client: PDOKClient,
        sample_polygon: Polygon,
    ) -> None:
        # When the user passes an explicit target_crs, we never silently fall
        # back -- we raise.
        gs = gpd.GeoSeries([sample_polygon], crs="OGC:CRS84")
        with pytest.raises(ValueError, match="supported CRS"):
            default_client.prepare_polygon(gs, target_crs="EPSG:4326")

    def test_empty_polygon_raises(self, default_client: PDOKClient) -> None:
        gs = gpd.GeoSeries([Polygon()], crs="OGC:CRS84")
        with pytest.raises(ValueError, match="polygon is empty"):
            default_client.prepare_polygon(gs, target_crs="OGC:CRS84")

    def test_unsupported_input_type_raises(
        self, default_client: PDOKClient
    ) -> None:
        with pytest.raises(TypeError, match="polygon must be"):
            default_client.prepare_polygon(
                "not a polygon", target_crs="OGC:CRS84",  # type: ignore[arg-type]
            )

    def test_multipolygon_accepted(
        self, default_client: PDOKClient
    ) -> None:
        # MultiPolygon is also valid input (e.g. disjoint AOIs).
        poly_a = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        poly_b = Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])
        mp = MultiPolygon([poly_a, poly_b])
        geom, uri = default_client.prepare_polygon(mp, target_crs="OGC:CRS84")
        assert uri == DEFAULT_CRS_URI
        assert geom.geom_type == "MultiPolygon"
        assert geom.area == pytest.approx(poly_a.area + poly_b.area)

    @pytest.mark.parametrize(
        "bad_geom",
        [
            Point(4.9, 52.38),
            LineString([(4.89, 52.37), (4.92, 52.39)]),
        ],
        ids=["point", "linestring"],
    )
    def test_non_polygonal_shapely_rejected(
        self, default_client: PDOKClient, bad_geom: BaseGeometry
    ) -> None:
        with pytest.raises(TypeError, match="Polygon or MultiPolygon"):
            default_client.prepare_polygon(bad_geom, target_crs="OGC:CRS84")

    def test_geoseries_of_lines_rejected(
        self, default_client: PDOKClient
    ) -> None:
        # A GeoSeries whose union resolves to a non-polygonal geometry must
        # also be rejected (unioning lines gives a (Multi)LineString).
        lines = gpd.GeoSeries(
            [LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 0)])],
            crs="OGC:CRS84",
        )
        with pytest.raises(TypeError, match="Polygon or MultiPolygon"):
            default_client.prepare_polygon(lines, target_crs="OGC:CRS84")
