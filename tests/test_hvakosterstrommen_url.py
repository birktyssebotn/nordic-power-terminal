from datetime import date

import pytest

from npt.data.connectors.hvakosterstrommen import HvakosterstrommenClient


def test_build_url_format():
    c = HvakosterstrommenClient()
    d = date(2026, 2, 14)
    assert c.build_url(d, "NO5") == "https://www.hvakosterstrommen.no/api/v1/prices/2026/02-14_NO5.json"


def test_build_url_case_insensitive():
    c = HvakosterstrommenClient()
    d = date(2025, 6, 1)
    assert c.build_url(d, "no1") == c.build_url(d, "NO1")


def test_build_url_invalid_zone_raises():
    c = HvakosterstrommenClient()
    with pytest.raises(ValueError, match="Invalid zone"):
        c.build_url(date(2025, 1, 1), "SE1")


def test_build_url_all_valid_zones():
    c = HvakosterstrommenClient()
    d = date(2025, 3, 15)
    for zone in ("NO1", "NO2", "NO3", "NO4", "NO5"):
        url = c.build_url(d, zone)
        assert zone in url
        assert "2025/03-15" in url
