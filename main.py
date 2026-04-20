import logging
import os
from typing import Any

import geopandas as gpd
import matplotlib
matplotlib.use("Agg")  # headless: write PNG without GUI backend
import matplotlib.pyplot as plt
from shapely.geometry import Polygon

from src import PDOKClient
from src.PDOK_clients.nwb_wegen import NWBWegen

# dir setup
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data/")
OUTPUT_DIR = os.path.join(ROOT_DIR, "output/")
LOG_DIR = os.path.join(ROOT_DIR, "logs/")
DIRS = [ROOT_DIR, DATA_DIR, OUTPUT_DIR, LOG_DIR]

# AOI as GeoJSON Polygon (lon/lat, WGS84 / OGC:CRS84). Goeree-Overflakkee region.
AOI_GEOJSON = os.path.join(DATA_DIR, "aoi.geojson")

# Hand-built lon/lat polygon (CRS84). Roughly the same Zeeland envelope.
INLINE_POLYGON = Polygon([
    (3.82, 51.62), (4.00, 51.60), (4.25, 51.63), (4.41, 51.70),
    (4.41, 51.80), (4.30, 51.86), (4.05, 51.86), (3.85, 51.82),
    (3.82, 51.72), (3.82, 51.62),
])


def main() -> None:
    setup_dirs()
    logger = setup_logger()
    logger.info("Setup PDOK Client")
    pdok = PDOKClient()

    inspect_service(pdok, logger)

    aoi = load_aoi(logger)

    # Each example writes <name>.gpkg + <name>.png into OUTPUT_DIR.
    run_example(
        pdok.nwb_wegen, logger,
        name="bbox",
        title="Wegvakken from a plain bounding box",
        kwargs={"bbox": (3.82, 51.62, 4.41, 51.86)},
        overlay=None,
    )
    run_example(
        pdok.nwb_wegen, logger,
        name="aoi_geojson",
        title="Wegvakken clipped to AOI loaded from GeoJSON",
        kwargs={"polygon": aoi},
        overlay=aoi,
    )
    run_example(
        pdok.nwb_wegen, logger,
        name="aoi_inline_shapely",
        title="Wegvakken clipped to a hand-built Shapely polygon",
        kwargs={
            "polygon": gpd.GeoSeries([INLINE_POLYGON], crs="OGC:CRS84"),
        },
        overlay=gpd.GeoSeries([INLINE_POLYGON], crs="OGC:CRS84"),
    )
    run_example(
        pdok.nwb_wegen, logger,
        name="aoi_provincie_only",
        title='Provincie-managed roads (wegbehsrt="P") in the AOI',
        kwargs={"polygon": aoi, "road_manager": "P"},
        overlay=aoi,
    )


# --- helpers ------------------------------------------------------------------
def inspect_service(pdok: PDOKClient, logger: logging.Logger) -> None:
    """Light read-only metadata calls; useful sanity check at startup."""
    logger.info("Get landing page NWB")
    pdok.nwb_wegen.get_landing_page()
    logger.info("Get conformance NWB")
    pdok.nwb_wegen.get_conformance()
    logger.info("Get collection NWB wegvakken")
    pdok.nwb_wegen.get_collection(collection="wegvakken")

    logger.info("Get wegvakken schema (JSON Schema for features)")
    schema = pdok.nwb_wegen.get_wegvakken_schema()
    fields = PDOKClient.json_schema_property_field_names(schema)
    logger.info(
        "wegvakken schema: %d property keys (e.g. %s ...)",
        len(fields), ", ".join(fields[:8]),
    )


def load_aoi(logger: logging.Logger) -> gpd.GeoDataFrame:
    """Load the AOI GeoJSON and tag it as OGC:CRS84.

    GeoJSON (RFC 7946) is lon/lat = OGC:CRS84; pyogrio defaults to EPSG:4326
    which PDOK rejects (different axis-order). Force CRS84 explicitly.
    """
    logger.info("Load AOI polygon: %s", AOI_GEOJSON)
    aoi = gpd.read_file(AOI_GEOJSON).set_crs("OGC:CRS84", allow_override=True)
    logger.info("AOI: %d feature(s), crs=%s", len(aoi), aoi.crs)
    return aoi


def run_example(
    nwb: NWBWegen,
    logger: logging.Logger,
    *,
    name: str,
    title: str,
    kwargs: dict[str, Any],
    overlay: gpd.GeoDataFrame | gpd.GeoSeries | None,
) -> None:
    """Fetch wegvakken with the given kwargs, save GPKG + plot under name."""
    logger.info("=" * 70)
    logger.info("Example %r: %s", name, title)
    logger.info("get_wegvakken kwargs: %s", {k: type(v).__name__ for k, v in kwargs.items()})

    gdf = nwb.get_wegvakken(progress=True, **kwargs)
    logger.info(
        "  -> rows=%d crs=%s columns=%s",
        len(gdf), gdf.crs, list(gdf.columns)[:6],
    )

    gdf_rd = gdf.to_crs(28992)
    overlay_rd = overlay.to_crs(28992) if overlay is not None else None

    gpkg_path = os.path.join(OUTPUT_DIR, f"{name}.gpkg")
    logger.info("  saving GeoPackage (EPSG:28992) -> %s", gpkg_path)
    if os.path.exists(gpkg_path):
        os.remove(gpkg_path)
    gdf_rd.to_file(gpkg_path, layer="wegvakken")
    if overlay_rd is not None:
        overlay_rd.to_file(gpkg_path, layer="aoi")

    png_path = os.path.join(OUTPUT_DIR, f"{name}.png")
    logger.info("  saving plot -> %s", png_path)
    fig, ax = plt.subplots(figsize=(10, 10), dpi=150)
    if overlay_rd is not None:
        overlay_boundary = (
            overlay_rd.boundary if hasattr(overlay_rd, "boundary") else overlay_rd
        )
        overlay_boundary.plot(ax=ax, color="#c0504d", linewidth=1.0)
    gdf_rd.plot(ax=ax, linewidth=0.5, color="#1f4e79")
    ax.set_title(f"{title}\n{len(gdf_rd):,} features (CRS {gdf_rd.crs.to_string()})")
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(png_path, bbox_inches="tight")
    plt.close(fig)


def setup_dirs() -> None:
    for dir in DIRS:
        os.makedirs(name=dir, exist_ok=True)


def setup_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(level="DEBUG")

    format_file = logging.Formatter(
        fmt="[%(levelname)s]  [%(asctime)s] : %(message)s",
        datefmt="%m-%d-%Y %I:%M:%S",
    )
    format_console = logging.Formatter(
        fmt="[%(levelname)s]  [%(asctime)s] : %(message)s",
        datefmt="%I:%M:%S",
    )

    file_handler = logging.FileHandler(
        filename=os.path.join(LOG_DIR, "log.log"),
        mode="w",
        encoding="utf-8",
    )
    file_handler.setFormatter(format_file)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(format_console)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    pdok_clients = logging.getLogger("src.PDOK_clients")
    pdok_clients.setLevel(logging.INFO)
    pdok_clients.addHandler(console_handler)
    pdok_clients.addHandler(file_handler)
    pdok_clients.propagate = False

    return logger


if __name__ == "__main__":
    main()
