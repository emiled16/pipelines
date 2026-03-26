from __future__ import annotations

from io import BytesIO
from urllib.error import HTTPError

from ingestion.providers.ofac_sanctions.checkpoint import OfacSanctionsCheckpoint
from ingestion.providers.ofac_sanctions.loader import (
    load_sanctions_entries,
    parse_sanctions_entries,
)


def test_parse_sanctions_entries_extracts_core_sdn_fields() -> None:
    xml_bytes = b"""
    <sdnList xmlns="urn:ofac:test">
      <publshInformation>
        <Publish_Date>03/25/2026</Publish_Date>
        <Record_Count>2</Record_Count>
      </publshInformation>
      <sdnEntry>
        <uid>100</uid>
        <firstName>Jane</firstName>
        <lastName>Doe</lastName>
        <sdnType>Individual</sdnType>
        <programList>
          <program>CYBER2</program>
          <program>SDGT</program>
        </programList>
        <akaList>
          <aka>
            <uid>200</uid>
            <type>a.k.a.</type>
            <category>strong</category>
            <firstName>J.</firstName>
            <lastName>Doe</lastName>
          </aka>
        </akaList>
        <addressList>
          <address>
            <uid>300</uid>
            <city>Toronto</city>
            <country>Canada</country>
          </address>
        </addressList>
        <nationalityList>
          <nationality><country>Canada</country></nationality>
        </nationalityList>
        <citizenshipList>
          <citizenship><country>France</country></citizenship>
        </citizenshipList>
        <dateOfBirthList>
          <dateOfBirthItem><dateOfBirth>1980-01-01</dateOfBirth></dateOfBirthItem>
        </dateOfBirthList>
        <placeOfBirthList>
          <placeOfBirthItem><placeOfBirth>Ottawa, Canada</placeOfBirth></placeOfBirthItem>
        </placeOfBirthList>
        <idList>
          <id>
            <uid>400</uid>
            <idType>Passport</idType>
            <idNumber>P123</idNumber>
            <idCountry>Canada</idCountry>
          </id>
        </idList>
        <remarks>Flagged entry</remarks>
      </sdnEntry>
      <sdnEntry>
        <uid>101</uid>
        <lastName>Example Shipping Ltd</lastName>
        <sdnType>Entity</sdnType>
      </sdnEntry>
    </sdnList>
    """

    entries = parse_sanctions_entries(xml_bytes)
    parsed_entries = list(entries)

    assert entries.publish_date == "03/25/2026"
    assert entries.record_count == 2
    assert [entry["id"] for entry in parsed_entries] == ["100", "101"]
    assert parsed_entries[0]["name"] == "Jane Doe"
    assert parsed_entries[0]["programs"] == ["CYBER2", "SDGT"]
    assert parsed_entries[0]["aliases"] == [
        {
            "uid": "200",
            "type": "a.k.a.",
            "category": "strong",
            "first_name": "J.",
            "last_name": "Doe",
            "name": "J. Doe",
        }
    ]
    assert parsed_entries[0]["addresses"] == [
        {"uid": "300", "city": "Toronto", "country": "Canada"}
    ]
    assert parsed_entries[0]["nationalities"] == ["Canada"]
    assert parsed_entries[0]["citizenships"] == ["France"]
    assert parsed_entries[0]["dates_of_birth"] == ["1980-01-01"]
    assert parsed_entries[0]["places_of_birth"] == ["Ottawa, Canada"]
    assert parsed_entries[0]["identifiers"] == [
        {
            "uid": "400",
            "id_type": "Passport",
            "id_number": "P123",
            "id_country": "Canada",
        }
    ]
    assert parsed_entries[1]["name"] == "Example Shipping Ltd"


def test_load_sanctions_entries_keeps_response_open_until_entries_are_consumed(monkeypatch) -> None:
    xml_bytes = b"""
    <sdnList>
      <publshInformation>
        <Publish_Date>03/25/2026</Publish_Date>
      </publshInformation>
      <sdnEntry><uid>100</uid><lastName>Example Shipping Ltd</lastName></sdnEntry>
    </sdnList>
    """
    response = _FakeResponse(
        xml_bytes,
        headers={"ETag": '"etag-1"', "Last-Modified": "Wed, 25 Mar 2026 10:15:00 GMT"},
    )

    monkeypatch.setattr(
        "ingestion.providers.ofac_sanctions.loader.urlopen",
        lambda request: response,
    )

    load_result = load_sanctions_entries("https://example.com/ofac.xml")

    assert response.closed is False
    assert load_result.etag == '"etag-1"'
    assert [entry["id"] for entry in load_result.entries] == ["100"]
    assert response.closed is True


def test_load_sanctions_entries_returns_not_modified_response(monkeypatch) -> None:
    checkpoint = OfacSanctionsCheckpoint(
        source_url="https://example.com/ofac.xml",
        etag='"etag-1"',
        last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
    )

    def raise_not_modified(request):
        raise HTTPError(request.full_url, 304, "Not Modified", hdrs={}, fp=None)

    monkeypatch.setattr("ingestion.providers.ofac_sanctions.loader.urlopen", raise_not_modified)

    load_result = load_sanctions_entries(checkpoint.source_url, checkpoint=checkpoint)

    assert load_result.not_modified is True
    assert list(load_result.entries) == []
    assert load_result.etag == '"etag-1"'
    assert load_result.last_modified == "Wed, 25 Mar 2026 10:15:00 GMT"


class _FakeResponse:
    def __init__(self, body: bytes, *, headers: dict[str, str]) -> None:
        self._stream = BytesIO(body)
        self.headers = headers
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True
