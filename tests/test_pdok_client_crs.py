import json

import pytest
from pyproj import CRS
from pyproj.exceptions import CRSError

from src.PDOK_clients.pdok_client import DEFAULT_CRS_URI, PDOKClient, _CRS_URIS

# PDOK CRS tests (valid crs inputs, various crs input types)
def test_null() -> None:
    input_crs = None
    expected_crs_uri = DEFAULT_CRS_URI
    client = PDOKClient(crs=input_crs)
    assert client.crs_uri == expected_crs_uri
    assert client.crs.equals(CRS(expected_crs_uri))


@pytest.mark.parametrize(
    ("input_crs", "expected_crs_uri"),
    [
        ("EPSG:28992", "http://www.opengis.net/def/crs/EPSG/0/28992"),
        ("EPSG:3857", "http://www.opengis.net/def/crs/EPSG/0/3857"),
        ("EPSG:4258", "http://www.opengis.net/def/crs/EPSG/0/4258"),
        (
            "http://www.opengis.net/def/crs/EPSG/0/28992",
            "http://www.opengis.net/def/crs/EPSG/0/28992",
        ),
    ],
    ids=["EPSG:28992", "EPSG:3857", "EPSG:4258", "URI-28992"],
)
def test_proj_string(
    input_crs: str, expected_crs_uri: str
) -> None:
    client = PDOKClient(crs=input_crs)
    assert client.crs_uri == expected_crs_uri
    assert client.crs.equals(CRS(expected_crs_uri))


@pytest.mark.parametrize(
    ("input_crs", "expected_crs_uri"),
    [
        (28992, "http://www.opengis.net/def/crs/EPSG/0/28992"),
        (("epsg", "28992"), "http://www.opengis.net/def/crs/EPSG/0/28992"),
    ],
    ids=["epsg-int-28992", "epsg-tuple-28992"],
)
def test_epsg_int(
    input_crs: object, expected_crs_uri: str
) -> None:
    client = PDOKClient(crs=input_crs)
    assert client.crs_uri == expected_crs_uri


def test_wkt_string() -> None:
    wkt = CRS("EPSG:28992").to_wkt()
    expected_crs_uri = "http://www.opengis.net/def/crs/EPSG/0/28992"
    client = PDOKClient(crs=wkt)
    assert client.crs_uri == expected_crs_uri


def test_json_string() -> None:
    json_str = CRS("EPSG:28992").to_json()
    expected_crs_uri = "http://www.opengis.net/def/crs/EPSG/0/28992"
    client = PDOKClient(crs=json_str)
    assert client.crs_uri == expected_crs_uri


def test_dict() -> None:
    input_crs = json.loads(CRS("EPSG:28992").to_json())
    expected_crs_uri = "http://www.opengis.net/def/crs/EPSG/0/28992"
    client = PDOKClient(crs=input_crs)
    assert client.crs_uri == expected_crs_uri


def test_crs_object() -> None:
    input_crs = CRS("EPSG:28992")
    expected_crs_uri = "http://www.opengis.net/def/crs/EPSG/0/28992"
    client = PDOKClient(crs=input_crs)
    assert client.crs_uri == expected_crs_uri
    assert client.crs.equals(input_crs)


def test_proj_str_default() -> None:
    input_crs = "OGC:CRS84"
    expected_crs_uri = DEFAULT_CRS_URI
    client = PDOKClient(crs=input_crs)
    assert client.crs_uri == expected_crs_uri


def test_allowed_uris() -> None:
    for input_crs in _CRS_URIS:
        expected_crs_uri = input_crs
        client = PDOKClient(crs=input_crs)
        assert client.crs_uri == expected_crs_uri


# Unsupported CRS values by Client
def test_unsupported_proj_string() -> None:
    input_crs = "EPSG:4326" #not supported
    with pytest.raises(ValueError, match="supported CRS"):
        PDOKClient(crs=input_crs)


def test_unsupported_crs_obj() -> None:
    input_crs = CRS("EPSG:4326") #not supported
    with pytest.raises(ValueError, match="supported CRS"):
        PDOKClient._resolve_crs_uri(input_crs)


# Invalid crs: pyprojt should reject before PDOKClient validation
@pytest.mark.parametrize(
    "input_crs",
    [
        "",
        "nonsense",
        {},
        ("foo", "bar", "baz"),
    ],
    ids=["empty-str", "garbage-str", "empty-dict", "bad-tuple"],
)
def test_invalid_crs_input(input_crs: object) -> None:
    with pytest.raises(CRSError):
        PDOKClient(crs=input_crs)