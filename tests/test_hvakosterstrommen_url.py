from datetime import date

from npt.data.connectors.hvakosterstrommen import HvakosterstrommenClient


def test_build_url_format():
    c = HvakosterstrommenClient()
    d = date(2026, 2, 14)
    assert c.build_url(d, "NO5") == "https://www.hvakosterstrommen.no/api/v1/prices/2026/02-14_NO5.json"