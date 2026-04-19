import logging
import os

import geopandas as gpd
import matplotlib
matplotlib.use("Agg")  # headless: write PNG without GUI backend
import matplotlib.pyplot as plt

from src import PDOKClient

# dir setup
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data/")
OUTPUT_DIR = os.path.join(ROOT_DIR, "output/")
LOG_DIR = os.path.join(ROOT_DIR, "logs/")
DIRS = [ROOT_DIR, DATA_DIR, OUTPUT_DIR, LOG_DIR]

# AOI as GeoJSON Polygon (lon/lat, WGS84 / OGC:CRS84). Goeree-Overflakkee region.
AOI_GEOJSON = os.path.join(DATA_DIR, "aoi.geojson")


def main() -> None:
    setup_dirs()
    logger = setup_logger()
    logger.info("Setup PDOK Client")
    pdok = PDOKClient()

    # NWB — common
    logger.info("Get landing page NWB")
    landing_page = pdok.nwb_wegen.get_landing_page()
    logger.info("Get service description NWB")
    service_description = pdok.nwb_wegen.get_service_description()
    logger.info("Get conformance NWB")
    conformance = pdok.nwb_wegen.get_conformance()

    # collections
    logger.info("Get collection NWB")
    collection = pdok.nwb_wegen.get_collection()
    logger.info("Get collection NWB hectopunten")
    collection_hecto = pdok.nwb_wegen.get_collection(collection="hectopunten")
    logger.info("Get collection NWB wegvakken")
    collection_wegvakken = pdok.nwb_wegen.get_collection(collection="wegvakken")

    logger.info("Get hectopunten schema")
    hectopunten_schema = pdok.nwb_wegen.get_hectopunten_schema()
    logger.debug("hectopunten schema title: %r", hectopunten_schema.get("title"))

    logger.info("Get wegvakken schema (JSON Schema for features)")
    wegvakken_schema = pdok.nwb_wegen.get_wegvakken_schema()
    schema_fields = PDOKClient.json_schema_property_field_names(wegvakken_schema)
    logger.info(
        "wegvakken schema: %d property keys (e.g. %s …)",
        len(schema_fields),
        ", ".join(schema_fields[:8]),
    )

    logger.info("Load AOI polygon: %s", AOI_GEOJSON)
    # GeoJSON (RFC 7946) is lon/lat = OGC:CRS84; pyogrio defaults to EPSG:4326
    # which PDOK rejects (different axis-order). Force CRS84 explicitly.
    aoi = gpd.read_file(AOI_GEOJSON).set_crs("OGC:CRS84", allow_override=True)
    logger.info("AOI: %d feature(s), crs=%s", len(aoi), aoi.crs)

    logger.info("Fetch wegvakken clipped to AOI -> GeoDataFrame")
    gdf = pdok.nwb_wegen.get_wegvakken(polygon=aoi, progress=True)
    logger.info(
        "GeoDataFrame: rows=%s crs=%s columns=%s",
        len(gdf), gdf.crs, list(gdf.columns)[:6],
    )

    gdf_rd = gdf.to_crs(28992)
    aoi_rd = aoi.to_crs(28992)

    gpkg_path = os.path.join(OUTPUT_DIR, "wegvakken.gpkg")
    logger.info("Save GeoPackage (EPSG:28992) -> %s", gpkg_path)
    gdf_rd.to_file(gpkg_path, layer="wegvakken")
    aoi_rd.to_file(gpkg_path, layer="aoi")

    png_path = os.path.join(OUTPUT_DIR, "wegvakken.png")
    logger.info("Save plot -> %s", png_path)
    fig, ax = plt.subplots(figsize=(10, 10), dpi=150)
    aoi_rd.boundary.plot(ax=ax, color="#c0504d", linewidth=1.0)
    gdf_rd.plot(ax=ax, linewidth=0.5, color="#1f4e79")
    ax.set_title(f"NWB wegvakken in AOI: {len(gdf_rd):,} features (CRS {gdf_rd.crs.to_string()})")
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
